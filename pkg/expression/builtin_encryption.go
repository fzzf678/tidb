// Copyright 2017 PingCAP, Inc.
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

package expression

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/md5" // #nosec G501
	"crypto/rand"
	"crypto/sha1" // #nosec G505
	"crypto/sha256"
	"crypto/sha512"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	"github.com/pingcap/tidb/pkg/expression/expropt"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/encrypt"
	pwdValidator "github.com/pingcap/tidb/pkg/util/password-validation"
	"github.com/pingcap/tipb/go-tipb"
)

var (
	_ functionClass = &aesDecryptFunctionClass{}
	_ functionClass = &aesEncryptFunctionClass{}
	_ functionClass = &compressFunctionClass{}
	_ functionClass = &decodeFunctionClass{}
	_ functionClass = &encodeFunctionClass{}
	_ functionClass = &md5FunctionClass{}
	_ functionClass = &passwordFunctionClass{}
	_ functionClass = &randomBytesFunctionClass{}
	_ functionClass = &sha1FunctionClass{}
	_ functionClass = &sha2FunctionClass{}
	_ functionClass = &uncompressFunctionClass{}
	_ functionClass = &uncompressedLengthFunctionClass{}
	_ functionClass = &validatePasswordStrengthFunctionClass{}
)

var (
	_ builtinFunc = &builtinAesDecryptSig{}
	_ builtinFunc = &builtinAesDecryptIVSig{}
	_ builtinFunc = &builtinAesEncryptSig{}
	_ builtinFunc = &builtinAesEncryptIVSig{}
	_ builtinFunc = &builtinCompressSig{}
	_ builtinFunc = &builtinMD5Sig{}
	_ builtinFunc = &builtinPasswordSig{}
	_ builtinFunc = &builtinRandomBytesSig{}
	_ builtinFunc = &builtinSHA1Sig{}
	_ builtinFunc = &builtinSHA2Sig{}
	_ builtinFunc = &builtinUncompressSig{}
	_ builtinFunc = &builtinUncompressedLengthSig{}
	_ builtinFunc = &builtinValidatePasswordStrengthSig{}
)

// aesModeAttr indicates that the key length and iv attribute for specific block_encryption_mode.
// keySize is the key length in bits and mode is the encryption mode.
// ivRequired indicates that initialization vector is required or not.
// nolint:structcheck
type aesModeAttr struct {
	modeName   string
	keySize    int
	ivRequired bool
}

var aesModes = map[string]*aesModeAttr{
	// TODO support more modes, permitted mode values are: ECB, CBC, CFB1, CFB8, CFB128, OFB
	"aes-128-ecb": {"ecb", 16, false},
	"aes-192-ecb": {"ecb", 24, false},
	"aes-256-ecb": {"ecb", 32, false},
	"aes-128-cbc": {"cbc", 16, true},
	"aes-192-cbc": {"cbc", 24, true},
	"aes-256-cbc": {"cbc", 32, true},
	"aes-128-ofb": {"ofb", 16, true},
	"aes-192-ofb": {"ofb", 24, true},
	"aes-256-ofb": {"ofb", 32, true},
	"aes-128-cfb": {"cfb", 16, true},
	"aes-192-cfb": {"cfb", 24, true},
	"aes-256-cfb": {"cfb", 32, true},
}

type aesDecryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesDecryptFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen()) // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode := ctx.GetBlockEncryptionMode()
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_decrypt")
		}
		sig := &builtinAesDecryptIVSig{bf, mode}
		sig.setPbCode(tipb.ScalarFuncSig_AesDecryptIV)
		return sig, nil
	}
	sig := &builtinAesDecryptSig{bf, mode}
	sig.setPbCode(tipb.ScalarFuncSig_AesDecrypt)
	return sig, nil
}

type builtinAesDecryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptSig) Clone() builtinFunc {
	newSig := &builtinAesDecryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.args) == 3 {
		tc := typeCtx(ctx)
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		tc.AppendWarning(errWarnOptionIgnored.FastGenByArgs("IV"))
	}

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "ecb":
		plainText, err = encrypt.AESDecryptWithECB([]byte(cryptStr), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type builtinAesDecryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesDecryptIVSig) Clone() builtinFunc {
	newSig := &builtinAesDecryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_DECRYPT(crypt_str, key_key, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesDecryptIVSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	cryptStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_decrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var plainText []byte
	switch b.modeName {
	case "cbc":
		plainText, err = encrypt.AESDecryptWithCBC([]byte(cryptStr), key, []byte(iv))
	case "ofb":
		plainText, err = encrypt.AESDecryptWithOFB([]byte(cryptStr), key, []byte(iv))
	case "cfb":
		plainText, err = encrypt.AESDecryptWithCFB([]byte(cryptStr), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(plainText), false, nil
}

type aesEncryptFunctionClass struct {
	baseFunctionClass
}

func (c *aesEncryptFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, c.verifyArgs(args)
	}
	argTps := make([]types.EvalType, 0, len(args))
	for range args {
		argTps = append(argTps, types.ETString)
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, argTps...)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(aes.BlockSize * (args[0].GetType(ctx.GetEvalCtx()).GetFlen()/aes.BlockSize + 1)) // At most.
	types.SetBinChsClnFlag(bf.tp)

	blockMode := ctx.GetBlockEncryptionMode()
	mode, exists := aesModes[strings.ToLower(blockMode)]
	if !exists {
		return nil, errors.Errorf("unsupported block encryption mode - %v", blockMode)
	}
	if mode.ivRequired {
		if len(args) != 3 {
			return nil, ErrIncorrectParameterCount.GenWithStackByArgs("aes_encrypt")
		}
		sig := &builtinAesEncryptIVSig{bf, mode}
		sig.setPbCode(tipb.ScalarFuncSig_AesEncryptIV)
		return sig, nil
	}
	sig := &builtinAesEncryptSig{bf, mode}
	sig.setPbCode(tipb.ScalarFuncSig_AesEncrypt)
	return sig, nil
}

type builtinAesEncryptSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptSig) Clone() builtinFunc {
	newSig := &builtinAesEncryptSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if !b.ivRequired && len(b.args) == 3 {
		tc := typeCtx(ctx)
		// For modes that do not require init_vector, it is ignored and a warning is generated if it is specified.
		tc.AppendWarning(errWarnOptionIgnored.FastGenByArgs("IV"))
	}

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "ecb":
		cipherText, err = encrypt.AESEncryptWithECB([]byte(str), key)
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type builtinAesEncryptIVSig struct {
	baseBuiltinFunc
	*aesModeAttr
}

func (b *builtinAesEncryptIVSig) Clone() builtinFunc {
	newSig := &builtinAesEncryptIVSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	newSig.aesModeAttr = b.aesModeAttr
	return newSig
}

// evalString evals AES_ENCRYPT(str, key_str, iv).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_aes-decrypt
func (b *builtinAesEncryptIVSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	// According to doc: If either function argument is NULL, the function returns NULL.
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	keyStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	iv, isNull, err := b.args[2].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(iv) < aes.BlockSize {
		return "", true, errIncorrectArgs.GenWithStack("The initialization vector supplied to aes_encrypt is too short. Must be at least %d bytes long", aes.BlockSize)
	}
	// init_vector must be 16 bytes or longer (bytes in excess of 16 are ignored)
	iv = iv[0:aes.BlockSize]

	key := encrypt.DeriveKeyMySQL([]byte(keyStr), b.keySize)
	var cipherText []byte
	switch b.modeName {
	case "cbc":
		cipherText, err = encrypt.AESEncryptWithCBC([]byte(str), key, []byte(iv))
	case "ofb":
		cipherText, err = encrypt.AESEncryptWithOFB([]byte(str), key, []byte(iv))
	case "cfb":
		cipherText, err = encrypt.AESEncryptWithCFB([]byte(str), key, []byte(iv))
	default:
		return "", true, errors.Errorf("unsupported block encryption mode - %v", b.modeName)
	}
	if err != nil {
		return "", true, nil
	}
	return string(cipherText), false, nil
}

type decodeFunctionClass struct {
	baseFunctionClass
}

func (c *decodeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	sig := &builtinDecodeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Decode)
	return sig, nil
}

type builtinDecodeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinDecodeSig) Clone() builtinFunc {
	newSig := &builtinDecodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals DECODE(str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_decode
func (b *builtinDecodeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	dataStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	decodeStr, err := encrypt.SQLDecode(dataStr, passwordStr)
	return decodeStr, false, err
}

type encodeFunctionClass struct {
	baseFunctionClass
}

func (c *encodeFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}

	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}

	bf.tp.SetFlen(args[0].GetType(ctx.GetEvalCtx()).GetFlen())
	sig := &builtinEncodeSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Encode)
	return sig, nil
}

type builtinEncodeSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinEncodeSig) Clone() builtinFunc {
	newSig := &builtinEncodeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals ENCODE(crypt_str, password_str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_encode
func (b *builtinEncodeSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	decodeStr, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	passwordStr, isNull, err := b.args[1].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	dataStr, err := encrypt.SQLEncode(decodeStr, passwordStr)
	return dataStr, false, err
}

type passwordFunctionClass struct {
	baseFunctionClass
}

func (c *passwordFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.PWDHashLen + 1)
	sig := &builtinPasswordSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Password)
	return sig, nil
}

type builtinPasswordSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinPasswordSig) Clone() builtinFunc {
	newSig := &builtinPasswordSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinPasswordSig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
func (b *builtinPasswordSig) evalString(ctx EvalContext, row chunk.Row) (val string, isNull bool, err error) {
	pass, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	if len(pass) == 0 {
		return "", false, nil
	}

	// We should append a warning here because function "PASSWORD" is deprecated since MySQL 5.7.6.
	// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_password
	tc := typeCtx(ctx)
	tc.AppendWarning(errDeprecatedSyntaxNoReplacement.FastGenByArgs("PASSWORD", ""))

	return auth.EncodePassword(pass), false, nil
}

type randomBytesFunctionClass struct {
	baseFunctionClass
}

func (c *randomBytesFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(1024) // Max allowed random bytes
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinRandomBytesSig{bf}
	return sig, nil
}

type builtinRandomBytesSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinRandomBytesSig) Clone() builtinFunc {
	newSig := &builtinRandomBytesSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals RANDOM_BYTES(len).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_random-bytes
func (b *builtinRandomBytesSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	val, isNull, err := b.args[0].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if val < 1 || val > 1024 {
		return "", false, types.ErrOverflow.GenWithStackByArgs("length", "random_bytes")
	}
	buf := make([]byte, val)
	//nolint: gosec
	if n, err := rand.Read(buf); err != nil {
		return "", true, err
	} else if int64(n) != val {
		return "", false, errors.New("fail to generate random bytes")
	}
	return string(buf), false, nil
}

type md5FunctionClass struct {
	baseFunctionClass
}

func (c *md5FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(32)
	sig := &builtinMD5Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_MD5)
	return sig, nil
}

type builtinMD5Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinMD5Sig) Clone() builtinFunc {
	newSig := &builtinMD5Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals a builtinMD5Sig.
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_md5
func (b *builtinMD5Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	arg, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	sum := md5.Sum([]byte(arg)) // #nosec G401
	hexStr := hex.EncodeToString(sum[:])

	return hexStr, false, nil
}

type sha1FunctionClass struct {
	baseFunctionClass
}

func (c *sha1FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(40)
	sig := &builtinSHA1Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SHA1)
	return sig, nil
}

type builtinSHA1Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSHA1Sig) Clone() builtinFunc {
	newSig := &builtinSHA1Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals SHA1(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha1
// The value is returned as a string of 40 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSHA1Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hasher := sha1.New() // #nosec G401
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

type sha2FunctionClass struct {
	baseFunctionClass
}

func (c *sha2FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString, types.ETInt)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(128) // sha512
	sig := &builtinSHA2Sig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_SHA2)
	return sig, nil
}

type builtinSHA2Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSHA2Sig) Clone() builtinFunc {
	newSig := &builtinSHA2Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

type sm3FunctionClass struct {
	baseFunctionClass
}

func (c *sm3FunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	charset, collate := ctx.GetCharsetInfo()
	bf.tp.SetCharset(charset)
	bf.tp.SetCollate(collate)
	bf.tp.SetFlen(40)
	sig := &builtinSM3Sig{bf}
	return sig, nil
}

type builtinSM3Sig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinSM3Sig) Clone() builtinFunc {
	newSig := &builtinSM3Sig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals Sm3Hash(str).
// The value is returned as a string of 70 hexadecimal digits, or NULL if the argument was NULL.
func (b *builtinSM3Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hasher := auth.NewSM3()
	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// Supported hash length of SHA-2 family
const (
	SHA0   = 0
	SHA224 = 224
	SHA256 = 256
	SHA384 = 384
	SHA512 = 512
)

// evalString evals SHA2(str, hash_length).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_sha2
func (b *builtinSHA2Sig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}
	hashLength, isNull, err := b.args[1].EvalInt(ctx, row)
	if isNull || err != nil {
		return "", isNull, err
	}

	var hasher hash.Hash
	switch int(hashLength) {
	case SHA0, SHA256:
		hasher = sha256.New()
	case SHA224:
		hasher = sha256.New224()
	case SHA384:
		hasher = sha512.New384()
	case SHA512:
		hasher = sha512.New()
	}
	if hasher == nil {
		return "", true, nil
	}

	_, err = hasher.Write([]byte(str))
	if err != nil {
		return "", true, err
	}
	return fmt.Sprintf("%x", hasher.Sum(nil)), false, nil
}

// deflate compresses a string using the DEFLATE format.
func deflate(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// inflate uncompresses a string using the DEFLATE format.
func inflate(compressStr []byte) ([]byte, error) {
	reader := bytes.NewReader(compressStr)
	var out bytes.Buffer
	r, err := zlib.NewReader(reader)
	if err != nil {
		return nil, err
	}
	/* #nosec G110 */
	if _, err = io.Copy(&out, r); err != nil {
		return nil, err
	}
	err = r.Close()
	return out.Bytes(), err
}

type compressFunctionClass struct {
	baseFunctionClass
}

func (c *compressFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	srcLen := args[0].GetType(ctx.GetEvalCtx()).GetFlen()
	compressBound := min(srcLen+(srcLen>>12)+(srcLen>>14)+(srcLen>>25)+13, mysql.MaxBlobWidth)
	bf.tp.SetFlen(compressBound)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinCompressSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Compress)
	return sig, nil
}

type builtinCompressSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinCompressSig) Clone() builtinFunc {
	newSig := &builtinCompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals COMPRESS(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_compress
func (b *builtinCompressSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}

	// According to doc: Empty strings are stored as empty strings.
	if len(str) == 0 {
		return "", false, nil
	}

	compressed, err := deflate([]byte(str))
	if err != nil {
		return "", true, nil
	}

	resultLength := 4 + len(compressed)

	// append "." if ends with space
	shouldAppendSuffix := compressed[len(compressed)-1] == 32
	if shouldAppendSuffix {
		resultLength++
	}

	buffer := make([]byte, resultLength)
	binary.LittleEndian.PutUint32(buffer, uint32(len(str)))
	copy(buffer[4:], compressed)

	if shouldAppendSuffix {
		buffer[len(buffer)-1] = '.'
	}

	return string(buffer), false, nil
}

type uncompressFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETString, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(mysql.MaxBlobWidth)
	types.SetBinChsClnFlag(bf.tp)
	sig := &builtinUncompressSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_Uncompress)
	return sig, nil
}

type builtinUncompressSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUncompressSig) Clone() builtinFunc {
	newSig := &builtinUncompressSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalString evals UNCOMPRESS(compressed_string).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompress
func (b *builtinUncompressSig) evalString(ctx EvalContext, row chunk.Row) (string, bool, error) {
	tc := typeCtx(ctx)
	payload, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return "", true, err
	}
	if len(payload) == 0 {
		return "", false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		tc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	length := binary.LittleEndian.Uint32([]byte(payload[0:4]))
	bytes, err := inflate([]byte(payload[4:]))
	if err != nil {
		tc.AppendWarning(errZlibZData)
		return "", true, nil
	}
	if length < uint32(len(bytes)) {
		tc.AppendWarning(errZlibZBuf)
		return "", true, nil
	}
	return string(bytes), false, nil
}

type uncompressedLengthFunctionClass struct {
	baseFunctionClass
}

func (c *uncompressedLengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(10)
	sig := &builtinUncompressedLengthSig{bf}
	sig.setPbCode(tipb.ScalarFuncSig_UncompressedLength)
	return sig, nil
}

type builtinUncompressedLengthSig struct {
	baseBuiltinFunc
	// NOTE: Any new fields added here must be thread-safe or immutable during execution,
	// as this expression may be shared across sessions.
	// If a field does not meet these requirements, set SafeToShareAcrossSession to false.
}

func (b *builtinUncompressedLengthSig) Clone() builtinFunc {
	newSig := &builtinUncompressedLengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// evalInt evals UNCOMPRESSED_LENGTH(str).
// See https://dev.mysql.com/doc/refman/5.7/en/encryption-functions.html#function_uncompressed-length
func (b *builtinUncompressedLengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	tc := typeCtx(ctx)
	payload, isNull, err := b.args[0].EvalString(ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	if len(payload) == 0 {
		return 0, false, nil
	}
	if len(payload) <= 4 {
		// corrupted
		tc.AppendWarning(errZlibZData)
		return 0, false, nil
	}
	return int64(binary.LittleEndian.Uint32([]byte(payload)[0:4])), false, nil
}

type validatePasswordStrengthFunctionClass struct {
	baseFunctionClass
}

func (c *validatePasswordStrengthFunctionClass) getFunction(ctx BuildContext, args []Expression) (builtinFunc, error) {
	if err := c.verifyArgs(args); err != nil {
		return nil, err
	}
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, types.ETString)
	if err != nil {
		return nil, err
	}
	bf.tp.SetFlen(21)
	sig := &builtinValidatePasswordStrengthSig{baseBuiltinFunc: bf}
	return sig, nil
}

type builtinValidatePasswordStrengthSig struct {
	baseBuiltinFunc
	expropt.SessionVarsPropReader
	expropt.CurrentUserPropReader
}

func (b *builtinValidatePasswordStrengthSig) Clone() builtinFunc {
	newSig := &builtinValidatePasswordStrengthSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (b *builtinValidatePasswordStrengthSig) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return b.SessionVarsPropReader.RequiredOptionalEvalProps() |
		b.CurrentUserPropReader.RequiredOptionalEvalProps()
}

// evalInt evals VALIDATE_PASSWORD_STRENGTH(str).
// See https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_validate-password-strength
func (b *builtinValidatePasswordStrengthSig) evalInt(ctx EvalContext, row chunk.Row) (int64, bool, error) {
	user, err := b.CurrentUser(ctx)
	if err != nil {
		return 0, true, err
	}

	vars, err := b.GetSessionVars(ctx)
	if err != nil {
		return 0, true, err
	}
	globalVars := vars.GlobalVarsAccessor
	str, isNull, err := b.args[0].EvalString(ctx, row)
	if err != nil || isNull {
		return 0, true, err
	} else if len([]rune(str)) < 4 {
		return 0, false, nil
	}
	if validation, err := globalVars.GetGlobalSysVar(vardef.ValidatePasswordEnable); err != nil {
		return 0, true, err
	} else if !variable.TiDBOptOn(validation) {
		return 0, false, nil
	}
	return b.validateStr(str, user, &globalVars)
}

func (b *builtinValidatePasswordStrengthSig) validateStr(str string, user *auth.UserIdentity, globalVars *variable.GlobalVarAccessor) (int64, bool, error) {
	if warn, err := pwdValidator.ValidateUserNameInPassword(str, user, globalVars); err != nil {
		return 0, true, err
	} else if len(warn) > 0 {
		return 0, false, nil
	}
	if warn, err := pwdValidator.ValidatePasswordLowPolicy(str, globalVars); err != nil {
		return 0, true, err
	} else if len(warn) > 0 {
		return 25, false, nil
	}
	if warn, err := pwdValidator.ValidatePasswordMediumPolicy(str, globalVars); err != nil {
		return 0, true, err
	} else if len(warn) > 0 {
		return 50, false, nil
	}
	if ok, err := pwdValidator.ValidateDictionaryPassword(str, globalVars); err != nil {
		return 0, true, err
	} else if !ok {
		return 75, false, nil
	}
	return 100, false, nil
}
