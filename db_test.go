package myminidb

import (
	"os"
	"sync"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestPut(t *testing.T) {
	// 需要将原有的db文件删掉
	convey.Convey("test put", t, func() {
		pwd, _ := os.Getwd()
		miniDB := &MiniDB{
			index:   make(map[string]int64),
			dirpath: pwd + string(os.PathSeparator) + "temp",
			mu:      &sync.RWMutex{},
		}
		miniDB.file, _ = NewDBFile(miniDB.dirpath)
		convey.Convey("test correct index", func() {
			offset := miniDB.file.Offset
			miniDB.Put([]byte("k1"), []byte("v1"))
			miniDB.Put([]byte("k2"), []byte("v2"))
			miniDB.Put([]byte("k3"), []byte("v3"))
			convey.So(len(miniDB.index), convey.ShouldEqual, 3)
			convey.So(miniDB.index["k2"], convey.ShouldEqual, offset+14)
			convey.So(miniDB.index["k3"], convey.ShouldEqual, offset+28)
		})

		convey.Convey("test get", func() {
			var (
				key      = []byte("k4")
				expected = []byte("v4")
			)
			miniDB.Put(key, expected)
			got, _ := miniDB.Get(key)
			convey.So(got, convey.ShouldResemble, expected)
		})

		convey.Convey("test update", func() {
			var (
				key      = []byte("k5")
				oriValue = []byte("whatever")
				expected = []byte("new")
			)
			miniDB.Put(key, oriValue)
			miniDB.Put(key, expected)
			got, _ := miniDB.Get(key)
			convey.So(got, convey.ShouldResemble, expected)
		})

	})
}

func TestDelete(t *testing.T) {
	convey.Convey("test delete", t, func() {
		pwd, _ := os.Getwd()
		miniDB := &MiniDB{
			index:   make(map[string]int64),
			dirpath: pwd + string(os.PathSeparator) + "temp",
			mu:      &sync.RWMutex{},
		}
		miniDB.file, _ = NewDBFile(miniDB.dirpath)

		convey.Convey("test delete not exist key", func() {
			var (
				key = []byte("k1")
			)
			got := miniDB.Delete(key)
			convey.So(got, convey.ShouldBeNil)
		})
		convey.Convey("test delete exist key", func() {
			var (
				key   = []byte("k2")
				value = []byte("v2")
			)
			err := miniDB.Put(key, value)
			convey.So(err, convey.ShouldBeNil)

			err = miniDB.Delete(key)
			convey.So(err, convey.ShouldBeNil)

			got, err := miniDB.Get(key)
			convey.So(err, convey.ShouldBeNil)
			convey.So(got, convey.ShouldBeEmpty)
		})
	})
}
