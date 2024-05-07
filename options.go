package bitcask_go

import "os"

// Options 用户配置项
type Options struct {
	// 数据库数据目录
	DirPath string
	// 数据文件的大小
	DataFileSize int64
	// 每次写数据是否持久化
	SyncWrites bool
	// 索引类型
	IndexType IndexerType
}

// IteratorOptions 索引迭代器配置项
type IteratorOptions struct {
	// 遍历前缀为指定值的 Key, 默认为空
	Prefix []byte
	// 是否反向遍历,默认 false 是正向
	Reverse bool
}

// WriteBatchOptions 批量写配置项
type WriteBatchOptions struct {
	// 一个批次当中最大的数据量
	MaxBatchNum uint

	// 提交事务时，是否进行持久化
	SyncWrites bool
}

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adaptive Radix Tree 自适应基数树索引
	ART
)

// 定义一个默认的 Options
var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, // 256MB
	SyncWrites:   false,
	IndexType:    BTree,
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
