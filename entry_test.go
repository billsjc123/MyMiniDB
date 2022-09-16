package myminidb

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestEntry(t *testing.T) {
	convey.Convey("Test Encode and Decode of entry", t, func() {
		entry := &Entry{
			Key:       []byte("abc"),
			Value:     []byte("12345"),
			KeySize:   3,
			ValueSize: 5,
			Mark:      0,
		}

		buf, err := entry.Encode()
		if err != nil {
			t.Errorf("encode err: %v", err)
			t.Fail()
		}

		newEntry := Decode(buf)
		convey.So(newEntry.KeySize, convey.ShouldEqual, entry.KeySize)
		convey.So(newEntry.ValueSize, convey.ShouldEqual, entry.ValueSize)
		convey.So(newEntry.Mark, convey.ShouldEqual, entry.Mark)
	})
}
