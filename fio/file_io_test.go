package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestNewFileIO(t *testing.T) {
	// 当使用wsl挂载本地时：项目在本地，但命令行访问路径只能通过/mnt/加本地路径访问
	path := filepath.Join("/mnt/d/golang/go_golandProject/bitcask-go/tmp", "0001.data")
	fio, err := NewFileIO(path)
	defer fio.Close()
	defer destroyFile(path) //清理掉测试产生的文件
	assert.Nil(t, err)
	assert.NotNil(t, fio)
}

func TestFileIO_Write(t *testing.T) {
	path := filepath.Join("/mnt/d/golang/go_golandProject/bitcask-go/tmp", "0001.data")
	fio, err := NewFileIO(path)
	defer fio.Close()
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("bitcask kv"))
	assert.Equal(t, 10, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("storage"))
	//t.Log(n, err)
	assert.Equal(t, 7, n)
	assert.Nil(t, err)
}

func TestFileIO_Read(t *testing.T) {
	path := filepath.Join("/mnt/d/golang/go_golandProject/bitcask-go/tmp", "0001.data")
	fio, err := NewFileIO(path)
	defer fio.Close()
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)

	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	b1 := make([]byte, 5)
	n, err := fio.Read(b1, 0)
	//t.Log(b, n)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)

	b2 := make([]byte, 5)
	n, err = fio.Read(b2, 5)
	//t.Log(string(b2), err)
	assert.Nil(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestFileIO_Sync(t *testing.T) {
	path := filepath.Join("/mnt/d/golang/go_golandProject/bitcask-go/tmp", "0001.data")
	fio, err := NewFileIO(path)
	defer fio.Close()
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
}

func TestFileIO_Close(t *testing.T) {
	path := filepath.Join("/mnt/d/golang/go_golandProject/bitcask-go/tmp", "0001.data")
	fio, err := NewFileIO(path)
	defer fio.Close()
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
}
