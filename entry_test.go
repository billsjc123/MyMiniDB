package myminidb

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestEntry(t *testing.T) {
	convey.Convey("Test Encode and Decode of entry", t, func() {
		entry := NewEntry([]byte("123"), []byte("abc"), Mark_Put)

		buf, err := entry.Encode()
		if err != nil {
			t.Errorf("encode err: %v", err)
			t.Fail()
		}

		newEntry := DecodeHeader(buf)
		convey.So(entry.GetSize(), convey.ShouldEqual, 16)
		convey.So(newEntry.KeySize, convey.ShouldEqual, entry.KeySize)
		convey.So(newEntry.ValueSize, convey.ShouldEqual, entry.ValueSize)
		convey.So(newEntry.Mark, convey.ShouldEqual, entry.Mark)
	})
}
