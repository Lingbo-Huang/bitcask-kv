package bitcask_go

import (
	"bitcask-go/data"
	"bitcask-go/index"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB bitcask 存储引擎实例
type DB struct {
	options    Options
	mu         *sync.RWMutex
	fileIds    []int                     // 文件 id, 只能在加载索引的时候使用
	activeFile *data.DataFile            // 当前活跃文件，可以用于写入
	olderFiles map[uint32]*data.DataFile // 旧的数据文件，只读
	index      index.Indexer             // 内存索引
	seqNo      uint64                    // 事务序列号,全局递增
}

// Open 打开 bitcask 存储引擎实例的方法
func Open(options Options) (*DB, error) {
	// 对用户传入的配置项进行校验
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	// 判断数据目录是否存在，如果不存在，则创建目录
	if _, err := os.Stat(options.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(options.DirPath, os.ModeDir); err != nil {
			return nil, err
		}
	}

	// 初始化 DB 实例结构体
	db := &DB{
		options:    options,
		mu:         new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.DataFile),
		index:      index.NewIndexer(options.IndexType),
	}

	// 加载数据文件
	if err := db.LoadDataFiles(); err != nil {
		return nil, err
	}

	// 从数据文件中加载索引
	if err := db.loadIndexFromDataFiles(); err != nil {
		return nil, err
	}
	return db, nil
}

// Close 关闭数据库
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	// 关闭当前活跃文件
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// 关闭旧的数据文件
	for _, of := range db.olderFiles {
		if err := of.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync 持久化数据文件
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.activeFile.Sync()
}

// Put 写入 Key/Value 数据， key 不能为空
func (db *DB) Put(key []byte, value []byte) error {
	// 判断key是否有效
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 构造LogRecord结构体
	logRecord := &data.LogRecord{
		Key:   logRecordKeyWithSeq(key, nonTransactionSeqNo),
		Value: value,
		Type:  data.LogRecordNormal,
	}

	// 追加写入导当前活跃数据文件当中
	pos, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return err
	}

	// 拿到索引信息之后，更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}

	return nil
}

// Delete 根据 key 删除数据
func (db *DB) Delete(key []byte) error {
	// 判断 key 的有效性
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	// 先检查 key 是否存在，如果不存在直接返回
	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	// 构造logRecord，标识其是被删除的
	logRecord := &data.LogRecord{Key: logRecordKeyWithSeq(key, nonTransactionSeqNo), Type: data.LogRecordDeleted}
	// 写入到数据文件里
	_, err := db.appendLogRecordWithLock(logRecord)
	if err != nil {
		return nil
	}

	// 从内存索引中将对应的 key 删除
	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

// ListKeys 获取所有的 key
func (db *DB) ListKeys() [][]byte {
	iterator := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		keys[idx] = iterator.Key()
		idx++
	}
	return keys
}

// Fold 获取所有的数据，并执行用户指定的操作
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	iterator := db.index.Iterator(false)
	for iterator.Rewind(); iterator.Valid(); iterator.Next() {
		value, err := db.getValueByPosition(iterator.Value())
		if err != nil {
			return err
		}
		// 执行用户的方法， 如果返回 false 则退出整个遍历
		if !fn(iterator.Key(), value) {
			break
		}
	}
	return nil
}

// Get 根据 key 读取数据
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// 判断 key 的有效型
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}

	// 从内存数据结构中取出 key 对应的索引信息
	logRecordPos := db.index.Get(key)
	// 如果没找到，说明 key 不存在内存索引中
	if logRecordPos == nil {
		return nil, ErrKeyNotFound
	}

	// 从数据文件中获取 value
	return db.getValueByPosition(logRecordPos)
}

// 因为不管是db的Get，还是iterator里的Seek都要有这段逻辑
// 根据拿到的数据位置*data.LogRecordPos 读取实际的数据 logRecord
// 所以提取出来
func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	// 根据文件 id 找到对应的数据文件
	var dataFile *data.DataFile
	if db.activeFile.FileId == logRecordPos.Fid {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}

	//数据文件为空，访问的数据文件既不在活跃文件里，也不在旧文件里
	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	// 根据偏移量，从数据文件读取数据
	logRecord, _, err := dataFile.ReadLogRecord(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}

	// 需要判断logRecord的类型，因为有可能是被删除的
	if logRecord.Type == data.LogRecordDeleted {
		return nil, ErrKeyNotFound
	}

	// 最后返回数据
	return logRecord.Value, nil
}

func (db *DB) appendLogRecordWithLock(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	return db.appendLogRecord(logRecord)
}

// 追加写数据到活跃数据文件中
// 因为有些函数调用改追加写数据函数时,本身已经加锁,所以这里面不能再加
func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {

	// 判断当前活跃文件是否存在，因为数据库在没有写入的时候是没有文件生成的
	// 如果为空，则初始化数据文件
	if db.activeFile == nil {
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}

	// logRecord是一个结构体，我们写入的时候是一个字节数组，所以需要编码
	encRecord, size := data.EncodeLogRecord(logRecord)
	// 如果写入的数据加上活跃数据文件当前数据量超出活跃文件的阈值，则关闭活跃文件，并打开新的文件
	if size+db.activeFile.WriteOff > db.options.DataFileSize {
		// 将当前活跃文件持久化
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}

		// 将当前活跃文件转换为一个旧的数据文件
		db.olderFiles[db.activeFile.FileId] = db.activeFile

		// 打开新的数据文件
		if err := db.setActiveDataFile(); err != nil {
			return nil, err
		}
	}
	// 写入数据
	writeOff := db.activeFile.WriteOff
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}

	// 写完要根据用户配置项，决定是否持久化
	if db.options.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}

	// 构造内存索引信息，返回出去
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// 设置当前活跃文件
// 在访问此方法前必须持有互斥锁
func (db *DB) setActiveDataFile() error {
	var initialFileId uint32 = 0
	// 当前活跃文件非空的时候，新建活跃文件
	if db.activeFile != nil { // 每个数据文件在新建的时候FileId是递增的
		initialFileId = db.activeFile.FileId + 1
	}
	// 打开新的数据文件
	dataFile, err := data.OpenDataFile(db.options.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// LoadDataFiles 从磁盘中加载数据文件
func (db *DB) LoadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	// 遍历目录中所有文件，找到所有以 .data 结尾的文件
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			// 将文件名称进行分割
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			// 数据目录可能被损坏了，导致出现了非数字命名的.data结尾的文件
			if err != nil {
				return ErrDataDirectoryCorrupted
			}
			fileIds = append(fileIds, fileId)
		}
	}

	// 对文件 id 进行排序，从小到大依次加载
	sort.Ints(fileIds)
	db.fileIds = fileIds

	// 遍历每一个id，打开对应的数据文件
	for i, fid := range fileIds {
		dataFile, err := data.OpenDataFile(db.options.DirPath, uint32(fid))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 { // 最后一个文件，id 是最大的，也就是活跃文件
			db.activeFile = dataFile
		} else { // 说明是旧数据文件
			db.olderFiles[uint32(fid)] = dataFile
		}
	}
	return nil
}

// 从数据文件中加载索引
// 遍历文件中的所有记录，并更新到内存索引中
func (db *DB) loadIndexFromDataFiles() error {
	// 没有文件，说明当前是空的数据库，直接返回
	if len(db.fileIds) == 0 {
		return nil
	}

	updateIndex := func(key []byte, typ data.LogRecordType, pos *data.LogRecordPos) {
		var ok bool
		// 因为logRecord可能是被删除的，所以需要进行判断
		if typ == data.LogRecordDeleted {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}

	// 暂存事务数据
	transactionRecords := make(map[uint64][]*data.TransactionRecord)
	var currentSeqNo = nonTransactionSeqNo

	// 遍历所有文件 id, 处理文件中的记录
	for i, fid := range db.fileIds {
		var fileId = uint32(fid)
		var dataFile *data.DataFile
		if fileId == db.activeFile.FileId {
			dataFile = db.activeFile
		} else {
			dataFile = db.olderFiles[fileId]
		}

		// 拿到文件之后，循环处理文件中所有内容
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			// 构建内存索引，把 logRecord 保存到内存索引中
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offset,
			}

			// 解析 key, 拿到事务序列号
			realKey, seqNo := parseLogRecordKey(logRecord.Key)
			if seqNo == nonTransactionSeqNo {
				// 非事务操作,直接更新内存索引
				updateIndex(realKey, logRecord.Type, logRecordPos)
			} else {
				// 事务操作
				// 事务完成提交,更新到内存索引当中
				if logRecord.Type == data.LogRecordTxnFinished {
					for _, txnRecord := range transactionRecords[seqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
					delete(transactionRecords, seqNo)
				} else {
					// 正常写入的数据,但还没判断到是否提交成功的标识,所以暂存起来
					logRecord.Key = realKey
					transactionRecords[seqNo] = append(transactionRecords[seqNo], &data.TransactionRecord{
						Record: logRecord,
						Pos:    logRecordPos,
					})
				}
			}

			// 更新事务序列号
			if seqNo > currentSeqNo {
				currentSeqNo = seqNo
			}

			// 递增 offset，下一次从新的位置开始读取
			offset += size

			// 如果读取的是活跃文件，需要记录偏移，维护 datafile 里的 WriteOff 变量，知道下次写入位置
			if i == len(db.fileIds)-1 {
				db.activeFile.WriteOff = offset
			}
		}
	}

	// 更新事务序列号
	db.seqNo = currentSeqNo
	return nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("database dir path is empty")
	}
	if options.DataFileSize <= 0 {
		return errors.New("database data file size must be greater than 0")
	}
	return nil
}
