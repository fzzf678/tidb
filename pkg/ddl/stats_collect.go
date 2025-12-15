package ddl

import (
	"fmt"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

type IndexStatsBuilder struct {
	sampleRate  float64
	r           *rand.Rand
	sampleItems []*statistics.SampleItem
	totalCnt    int64
	nullCnt     int64
	totalSize   int64 // sampled total byte size
	colInfo     *model.ColumnInfo
	colSize     int64
	fmSketch    *statistics.FMSketch
	scCtx       sessionctx.Context
}

func (s *IndexStatsBuilder) sampleIndexVal(idxData types.Datum, tz *time.Location) {
	s.totalCnt++
	// null value count
	if idxData.IsNull() {
		s.nullCnt++
		fmt.Println("index value is null")
		return
	}
	// fm sketch
	stmtCtx := s.scCtx.GetSessionVars().StmtCtx
	tz = time.FixedZone("UTC+88", 8*60*60)
	stmtCtx.SetTimeZone(tz)
	//s.fmSketch.InsertValue(stmtCtx, idxData)
	//key, err := codec.EncodeKey(stmtCtx.TimeZone(), nil, idxData)
	//if err != nil {
	//	logutil.BgLogger().Error("encode index value failed", zap.Error(err))
	//	return
	//}

	s.fmSketch.InsertIndexVal(stmtCtx, idxData)

	ks, vs := s.fmSketch.KV()
	fmt.Println("fm sketch hashset by index", ks, vs)
	// 13834778063021667559 15195375777814662037 14987360377986231656 17746585004847908418

	// 3243239667579247066 9283639292145088825 7048153190574704287 8121726991456836516

	// sample
	if s.collect() {
		s.totalSize += s.colSize

		b := make([]byte, 0, 8)
		var err error
		b, err = codec.EncodeKey(tz, b, idxData)
		if err != nil {
			// log error
			logutil.BgLogger().Error("encode index value failed", zap.Error(err))
			return
		}
		s.sampleItems = append(s.sampleItems, &statistics.SampleItem{
			Value: types.NewBytesDatum(b),
		})
	}
}

func (s *IndexStatsBuilder) buildStats() {
	collector := &statistics.SampleCollector{
		Samples:   s.sampleItems,
		NullCount: s.nullCnt,
		Count:     s.totalCnt - s.nullCnt,
		//FMSketch:  task.rootRowCollector.Base().FMSketches[task.slicePos],
		TotalSize: s.totalSize,
		//MemSize:   collectorMemSize,
	}

	hist, topn, err := statistics.BuildHistAndTopN(s.scCtx,
		256, 100, s.colInfo.ID, collector, &s.colInfo.FieldType,
		false, nil, false)
	if err != nil {
		logutil.BgLogger().Error("build index stats failed", zap.Error(err))
		return
	}
	fmt.Sprintf("hist: %+v, topn: %+v", hist, topn)

}

func (s *IndexStatsBuilder) collect() bool {
	return true
	return s.r.Float64() < s.sampleRate
}

func getTypeByteSize(tp types.FieldType) int64 {
	var typeSize int64

	switch tp.GetType() {
	case mysql.TypeLong:
		typeSize = 8 // ?
	case mysql.TypeLonglong:
		typeSize = 8
	default:
		logutil.BgLogger().Error("unsupported index column type for size estimation")
	}

	return typeSize
}
