// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collate

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/stretchr/testify/require"
)

type compareTable struct {
	Left   string
	Right  string
	Expect []int
}

type keyTable struct {
	Str    string
	Expect [][]byte
}

func testCompareTable(t *testing.T, collations []string, tests []compareTable) {
	for i, c := range collations {
		collator := GetCollator(c)
		for _, table := range tests {
			comment := fmt.Sprintf("Compare Left: %v Right: %v, Using %v", table.Left, table.Right, c)
			require.Equal(t, table.Expect[i], collator.Compare(table.Left, table.Right), comment)
		}
	}
}

func testKeyTable(t *testing.T, collations []string, tests []keyTable) {
	for i, c := range collations {
		collator := GetCollator(c)
		for _, test := range tests {
			comment := fmt.Sprintf("key %v, using %v", test.Str, c)
			require.Equal(t, test.Expect[i], collator.Key(test.Str), comment)
		}
	}
}

func TestUTF8CollatorCompare(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	collations := []string{"binary", "utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_0900_ai_ci", "utf8mb4_0900_bin", "gbk_bin", "gbk_chinese_ci"}
	tests := []compareTable{
		{"a", "b", []int{-1, -1, -1, -1, -1, -1, -1, -1}},
		{"a", "A", []int{1, 1, 0, 0, 0, 1, 1, 0}},
		{"À", "A", []int{1, 1, 0, 0, 0, 1, -1, -1}},
		{"abc", "abc", []int{0, 0, 0, 0, 0, 0, 0, 0}},
		{"abc", "ab", []int{1, 1, 1, 1, 1, 1, 1, 1}},
		{"😜", "😃", []int{1, 1, 0, 0, 1, 1, 0, 0}},
		{"a", "a ", []int{-1, 0, 0, 0, -1, -1, 0, 0}},
		{"a ", "a  ", []int{-1, 0, 0, 0, -1, -1, 0, 0}},
		{"a\t", "a", []int{1, 1, 1, 1, 1, 1, 1, 1}},
		{"ß", "s", []int{1, 1, 0, 1, 1, 1, -1, -1}},
		{"ß", "ss", []int{1, 1, -1, 0, 0, 1, -1, -1}},
		{"啊", "吧", []int{1, 1, 1, 1, 1, 1, -1, -1}},
		{"中文", "汉字", []int{-1, -1, -1, -1, -1, -1, 1, 1}},
		{"æ", "ae", []int{1, 1, 1, 1, 0, 1, -1, -1}},
		{"Å", "A", []int{1, 1, 1, 0, 0, 1, 1, 1}},
		{"Å", "A", []int{1, 1, 0, 0, 0, 1, -1, -1}},
		{"\U0001730F", "啊", []int{1, 1, 1, 1, -1, 1, -1, -1}},
		{"가", "㉡", []int{1, 1, 1, 1, -1, 1, 0, 0}},
		{"갟", "감1", []int{1, 1, 1, 1, 1, 1, -1, -1}},
		{"\U000FFFFE", "\U000FFFFF", []int{-1, -1, 0, 0, -1, -1, 0, 0}},
	}
	testCompareTable(t, collations, tests)
}

func TestUTF8CollatorKey(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	collations := []string{"binary", "utf8mb4_bin", "utf8mb4_general_ci", "utf8mb4_unicode_ci", "utf8mb4_0900_ai_ci", "utf8mb4_0900_bin", "gbk_bin", "gbk_chinese_ci"}
	tests := []keyTable{
		{"a", [][]byte{{0x61}, {0x61}, {0x0, 0x41}, {0x0E, 0x33}, {0x1C, 0x47}, {0x61}, {0x61}, {0x41}}},
		{"A", [][]byte{{0x41}, {0x41}, {0x0, 0x41}, {0x0E, 0x33}, {0x1C, 0x47}, {0x41}, {0x41}, {0x41}}},
		{"Foo © bar 𝌆 baz ☃ qux", [][]byte{
			{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78},
			{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78},
			{0x0, 0x46, 0x0, 0x4f, 0x0, 0x4f, 0x0, 0x20, 0x0, 0xa9, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x52, 0x0, 0x20, 0xff, 0xfd, 0x0, 0x20, 0x0, 0x42, 0x0, 0x41, 0x0, 0x5a, 0x0, 0x20, 0x26, 0x3, 0x0, 0x20, 0x0, 0x51, 0x0, 0x55, 0x0, 0x58},
			{0x0E, 0xB9, 0x0F, 0x82, 0x0F, 0x82, 0x02, 0x09, 0x02, 0xC5, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x0F, 0xC0, 0x02, 0x09, 0xFF, 0xFD, 0x02, 0x09, 0x0E, 0x4A, 0x0E, 0x33, 0x10, 0x6A, 0x02, 0x09, 0x06, 0xFF, 0x02, 0x09, 0x0F, 0xB4, 0x10, 0x1F, 0x10, 0x5A},
			{0x1c, 0xe5, 0x1d, 0xdd, 0x1d, 0xdd, 0x2, 0x9, 0x5, 0x84, 0x2, 0x9, 0x1c, 0x60, 0x1c, 0x47, 0x1e, 0x33, 0x2, 0x9, 0xe, 0xf0, 0x2, 0x9, 0x1c, 0x60, 0x1c, 0x47, 0x1f, 0x21, 0x2, 0x9, 0x9, 0x1b, 0x2, 0x9, 0x1e, 0x21, 0x1e, 0xb5, 0x1e, 0xff},
			{0x46, 0x6f, 0x6f, 0x20, 0xc2, 0xa9, 0x20, 0x62, 0x61, 0x72, 0x20, 0xf0, 0x9d, 0x8c, 0x86, 0x20, 0x62, 0x61, 0x7a, 0x20, 0xe2, 0x98, 0x83, 0x20, 0x71, 0x75, 0x78},
			{0x46, 0x6f, 0x6f, 0x20, 0x3f, 0x20, 0x62, 0x61, 0x72, 0x20, 0x3f, 0x20, 0x62, 0x61, 0x7a, 0x20, 0x3f, 0x20, 0x71, 0x75, 0x78},
			{0x46, 0x4f, 0x4f, 0x20, 0x3f, 0x20, 0x42, 0x41, 0x52, 0x20, 0x3f, 0x20, 0x42, 0x41, 0x5a, 0x20, 0x3f, 0x20, 0x51, 0x55, 0x58},
		}},
		{"a ", [][]byte{{0x61, 0x20}, {0x61}, {0x0, 0x41}, {0x0E, 0x33}, {0x1c, 0x47, 0x2, 0x9}, {0x61, 0x20}, {0x61}, {0x41}}},
		{"ﷻ", [][]byte{
			{0xEF, 0xB7, 0xBB},
			{0xEF, 0xB7, 0xBB},
			{0xFD, 0xFB},
			{0x13, 0x5E, 0x13, 0xAB, 0x02, 0x09, 0x13, 0x5E, 0x13, 0xAB, 0x13, 0x50, 0x13, 0xAB, 0x13, 0xB7},
			{0x23, 0x25, 0x23, 0x9c, 0x2, 0x9, 0x23, 0x25, 0x23, 0x9c, 0x23, 0xb, 0x23, 0x9c, 0x23, 0xb1},
			{0xEF, 0xB7, 0xBB},
			{0x3f},
			{0x3F},
		}},
		{"中文", [][]byte{
			{0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87},
			{0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87},
			{0x4E, 0x2D, 0x65, 0x87},
			{0xFB, 0x40, 0xCE, 0x2D, 0xFB, 0x40, 0xE5, 0x87},
			{0xFB, 0x40, 0xCE, 0x2D, 0xfB, 0x40, 0xE5, 0x87},
			{0xE4, 0xB8, 0xAD, 0xE6, 0x96, 0x87},
			{0xD6, 0xD0, 0xCE, 0xC4},
			{0xD3, 0x21, 0xC1, 0xAD},
		}},
		{"갟감1", [][]byte{
			{0xea, 0xb0, 0x9f, 0xea, 0xb0, 0x90, 0x31},
			{0xea, 0xb0, 0x9f, 0xea, 0xb0, 0x90, 0x31},
			{0xac, 0x1f, 0xac, 0x10, 0x0, 0x31},
			{0xfb, 0xc1, 0xac, 0x1f, 0xfb, 0xc1, 0xac, 0x10, 0xe, 0x2a},
			{0x3b, 0xf5, 0x3c, 0x74, 0x3c, 0xd3, 0x3b, 0xf5, 0x3c, 0x73, 0x3c, 0xe0, 0x1c, 0x3e},
			{0xea, 0xb0, 0x9f, 0xea, 0xb0, 0x90, 0x31},
			{0x3f, 0x3f, 0x31},
			{0x3f, 0x3f, 0x31},
		}},
		{"\U000FFFFE\U000FFFFF", [][]byte{
			{0xf3, 0xbf, 0xbf, 0xbe, 0xf3, 0xbf, 0xbf, 0xbf},
			{0xf3, 0xbf, 0xbf, 0xbe, 0xf3, 0xbf, 0xbf, 0xbf},
			{0xff, 0xfd, 0xff, 0xfd},
			{0xff, 0xfd, 0xff, 0xfd},
			{0xfb, 0xdf, 0xff, 0xfe, 0xfb, 0xdf, 0xff, 0xff},
			{0xf3, 0xbf, 0xbf, 0xbe, 0xf3, 0xbf, 0xbf, 0xbf},
			{0x3f, 0x3f},
			{0x3f, 0x3f},
		}},
	}
	testKeyTable(t, collations, tests)
}

func TestSetNewCollateEnabled(t *testing.T) {
	defer SetNewCollationEnabledForTest(false)

	SetNewCollationEnabledForTest(true)
	require.True(t, NewCollationEnabled())
}

func TestRewriteAndRestoreCollationID(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	require.Equal(t, int32(-5), RewriteNewCollationIDIfNeeded(5))
	require.Equal(t, int32(-5), RewriteNewCollationIDIfNeeded(-5))
	require.Equal(t, int32(5), RestoreCollationIDIfNeeded(-5))
	require.Equal(t, int32(5), RestoreCollationIDIfNeeded(5))

	SetNewCollationEnabledForTest(false)
	require.Equal(t, int32(5), RewriteNewCollationIDIfNeeded(5))
	require.Equal(t, int32(-5), RewriteNewCollationIDIfNeeded(-5))
	require.Equal(t, int32(5), RestoreCollationIDIfNeeded(5))
	require.Equal(t, int32(-5), RestoreCollationIDIfNeeded(-5))
}

func TestGetCollator(t *testing.T) {
	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	require.IsType(t, &binCollator{}, GetCollator("binary"))
	require.IsType(t, &binPaddingCollator{}, GetCollator("utf8mb4_bin"))
	require.IsType(t, &binPaddingCollator{}, GetCollator("utf8_bin"))
	require.IsType(t, &generalCICollator{}, GetCollator("utf8mb4_general_ci"))
	require.IsType(t, &generalCICollator{}, GetCollator("utf8_general_ci"))
	require.IsType(t, &unicodeCICollator{}, GetCollator("utf8mb4_unicode_ci"))
	require.IsType(t, &unicodeCICollator{}, GetCollator("utf8_unicode_ci"))
	require.IsType(t, &zhPinyinTiDBASCSCollator{}, GetCollator("utf8mb4_zh_pinyin_tidb_as_cs"))
	require.IsType(t, &unicode0900AICICollator{}, GetCollator("utf8mb4_0900_ai_ci"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8mb4_0900_bin"))
	require.IsType(t, &binPaddingCollator{}, GetCollator("default_test"))
	require.IsType(t, &binCollator{}, GetCollatorByID(63))
	require.IsType(t, &binPaddingCollator{}, GetCollatorByID(46))
	require.IsType(t, &binPaddingCollator{}, GetCollatorByID(83))
	require.IsType(t, &generalCICollator{}, GetCollatorByID(45))
	require.IsType(t, &generalCICollator{}, GetCollatorByID(33))
	require.IsType(t, &unicodeCICollator{}, GetCollatorByID(224))
	require.IsType(t, &unicodeCICollator{}, GetCollatorByID(192))
	require.IsType(t, &unicode0900AICICollator{}, GetCollatorByID(255))
	require.IsType(t, &zhPinyinTiDBASCSCollator{}, GetCollatorByID(2048))
	require.IsType(t, &binPaddingCollator{}, GetCollatorByID(9999))

	SetNewCollationEnabledForTest(false)
	require.IsType(t, &derivedBinCollator{}, GetCollator("binary"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8mb4_bin"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8_bin"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8mb4_general_ci"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8_general_ci"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8mb4_unicode_ci"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8_unicode_ci"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8mb4_zh_pinyin_tidb_as_cs"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("utf8mb4_0900_ai_ci"))
	require.IsType(t, &derivedBinCollator{}, GetCollator("default_test"))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(63))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(46))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(83))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(45))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(33))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(224))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(255))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(309))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(192))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(2048))
	require.IsType(t, &derivedBinCollator{}, GetCollatorByID(9999))

	SetNewCollationEnabledForTest(true)
	defer SetNewCollationEnabledForTest(false)
	require.IsType(t, &gbkBinCollator{}, GetCollator("gbk_bin"))
	require.IsType(t, &gbkBinCollator{}, GetCollatorByID(87))
}

type collator interface {
	Compare(a, b string) int
	Key(str string) []byte
}

func TestCampareInvalidUTF8Rune(t *testing.T) {
	collaters := []collator{
		&generalCICollator{},                                   // index: 0
		&unicode0900AICICollator{},                             // index: 1
		&unicodeCICollator{},                                   // index: 2
		&gbkChineseCICollator{},                                // index: 3
		&gb18030BinCollator{charset.NewCustomGB18030Encoder()}, // index: 4
		&gbkBinCollator{charset.NewCustomGBKEncoder()},         // index: 5
	}

	for i, c := range collaters {
		require.Equal(t, c.Compare(string([]byte{0xff}), string([]byte{0xff})), 0)
		require.Equal(t, c.Compare(string([]byte{0xff}), string([]byte{0xfe})), 0)
		// For `gbk_bin` and `gb18030_bin`, it will use 0x3f instead of invalid utf8 rune.
		if i < 4 {
			require.Equal(t, c.Compare(string([]byte{0xff}), string([]byte{0xff, 0x3e})), 0)
			require.Equal(t, c.Compare(string([]byte{0x3e, 0xff}), string([]byte{0x3e, 0xff, 0x3e})), 0)
		}
		require.NotNil(t, c.Key(string([]byte{0xff})))
		require.NotNil(t, c.Key(string([]byte{0x3e, 0xff})))
		require.NotNil(t, c.Key(string([]byte{0x3e, 0xff, 0x3e})))
	}
}
