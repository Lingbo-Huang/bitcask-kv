package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	t.Log(n1)
	/*
		header length: 7, crc: 2532332136
		    log_record_test.go:16: [104 82 240 150 0 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
		    log_record_test.go:17: 21
	*/
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	// value 为空的情况
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n2 := EncodeLogRecord(rec2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	t.Log(res2)
	t.Log(n2)
	/*
		header length: 7, crc: 240712713
		    log_record_test.go:35: [9 252 88 14 0 8 0 110 97 109 101]
		    log_record_test.go:36: 11
	*/

	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	res3, n3 := EncodeLogRecord(rec3)
	t.Log(res3)
	t.Log(n3)
	/*
		header length: 7, crc: 290887979
		    log_record_test.go:50: [43 153 86 17 1 8 20 110 97 109 101 98 105 116 99 97 115 107 45 103 111]
		    log_record_test.go:51: 21
	*/
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size1 := decodeLogRecordHeader(headerBuf1)
	t.Log(h1)
	t.Log(size1)
	/*
	   log_record_test.go:34: &{2532332136 0 4 10}
	   log_record_test.go:35: 7
	*/
	assert.NotNil(t, h1)
	assert.Equal(t, uint32(2532332136), h1.crc)
	assert.Equal(t, uint8(0), h1.recordType)
	assert.Equal(t, uint32(4), h1.keySize)
	assert.Equal(t, uint32(10), h1.valueSize)
	assert.Equal(t, int64(7), size1)

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := decodeLogRecordHeader(headerBuf2)
	t.Log(h2)
	t.Log(size2)

	assert.NotNil(t, h2)
	assert.Equal(t, uint32(240712713), h2.crc)
	assert.Equal(t, uint8(0), h2.recordType)
	assert.Equal(t, uint32(4), h2.keySize)
	assert.Equal(t, uint32(0), h2.valueSize)
	assert.Equal(t, int64(7), size2)

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size3 := decodeLogRecordHeader(headerBuf3)
	t.Log(h3)
	t.Log(size3)
	assert.NotNil(t, h3)
	assert.Equal(t, uint32(290887979), h3.crc)
	assert.Equal(t, uint8(1), h3.recordType)
	assert.Equal(t, uint32(4), h3.keySize)
	assert.Equal(t, uint32(10), h3.valueSize)
	assert.Equal(t, int64(7), size3)
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	t.Log(crc1)
	assert.Equal(t, uint32(2532332136), crc1)

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	t.Log(crc2)
	assert.Equal(t, uint32(240712713), crc2)

	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	crc3 := getLogRecordCRC(rec3, headerBuf3[crc32.Size:])
	t.Log(crc3)
	assert.Equal(t, uint32(290887979), crc3)
}
