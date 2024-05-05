package bitcask_go

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

type IndexerType = int8

const (
	// BTree 索引
	BTree IndexerType = iota + 1

	// ART Adaptive Radix Tree 自适应基数树索引
	ART
)
