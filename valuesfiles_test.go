package valuestore

import (
	"encoding/binary"
	"io"
	"testing"
)

type memBuf struct {
	buf []byte
}

type memFile struct {
	buf *memBuf
	pos int64
}

func (f *memFile) Read(p []byte) (int, error) {
	n := copy(p, f.buf.buf[f.pos:])
	if n == 0 {
		return 0, io.EOF
	}
	f.pos += int64(n)
	return n, nil
}

func (f *memFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		f.pos = offset
	case 1:
		f.pos += offset
	case 2:
		f.pos = int64(len(f.buf.buf)) + offset
	}
	return f.pos, nil
}

func (f *memFile) Write(p []byte) (int, error) {
	pl := int64(len(p))
	if int64(len(f.buf.buf))-f.pos < pl {
		buf := make([]byte, int64(len(f.buf.buf))-f.pos+pl)
		copy(buf, f.buf.buf)
		copy(buf[f.pos:], p)
		f.buf.buf = buf
		f.pos += pl
		return int(pl), nil
	}
	copy(f.buf.buf[f.pos:], p)
	f.pos += pl
	return int(pl), nil
}

func (f *memFile) Close() error {
	return nil
}

func TestValuesFileReading(t *testing.T) {
	vs := New(nil)
	buf := &memBuf{buf: []byte("0123456789abcdef")}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	vf := newValuesFile(vs, 12345, openReadSeeker)
	if vf == nil {
		t.Fatal("")
	}
	tsn := vf.timestampnano()
	if tsn != 12345 {
		t.Fatal(tsn)
	}
	ts, v, err := vf.read(1, 2, 0x300, 4, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "45678" {
		t.Fatal(string(v))
	}
	ts, v, err = vf.read(1, 2, 0x300|_TSB_DELETION, 4, 5, nil)
	if err != ErrNotFound {
		t.Fatal(err)
	}
	if ts != 0x300|_TSB_DELETION {
		t.Fatal(ts)
	}
	if v != nil {
		t.Fatal(v)
	}
	ts, v, err = vf.read(1, 2, 0x300, 4, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "45678" {
		t.Fatal(string(v))
	}
	_, _, err = vf.read(1, 2, 0x300, 12, 5, nil)
	if err != io.EOF {
		t.Fatal(err)
	}
	ts, v, err = vf.read(1, 2, 0x300, 4, 5, []byte("testing"))
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "testing45678" {
		t.Fatal(string(v))
	}
	v = make([]byte, 0, 50)
	ts, v, err = vf.read(1, 2, 0x300, 4, 5, v)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "45678" {
		t.Fatal(string(v))
	}
	ts, v, err = vf.read(1, 2, 0x300, 4, 5, v)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "4567845678" {
		t.Fatal(string(v))
	}
}

func TestValuesFileWritingEmpty(t *testing.T) {
	vs := New(nil)
	buf := &memBuf{}
	createWriteCloser := func(name string) (io.WriteCloser, error) {
		return &memFile{buf: buf}, nil
	}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	vf := createValuesFile(vs, createWriteCloser, openReadSeeker)
	if vf == nil {
		t.Fatal("")
	}
	vf.close()
	if len(buf.buf) != 52 {
		t.Fatal(len(buf.buf))
	}
	if string(buf.buf[:28]) != "VALUESTORE v0               " {
		t.Fatal(string(buf.buf[:28]))
	}
	if binary.BigEndian.Uint32(buf.buf[28:]) != vs.checksumInterval {
		t.Fatal(binary.BigEndian.Uint32(buf.buf[28:]), vs.checksumInterval)
	}
	if binary.BigEndian.Uint32(buf.buf[32:]) != 0 { // unused at this time
		t.Fatal(binary.BigEndian.Uint32(buf.buf[32:]))
	}
	if binary.BigEndian.Uint64(buf.buf[36:]) != 32 { // last offset, 0 past header
		t.Fatal(binary.BigEndian.Uint64(buf.buf[36:]))
	}
	if string(buf.buf[44:48]) != "TERM" {
		t.Fatal(string(buf.buf[44:48]))
	}
	if binary.BigEndian.Uint32(buf.buf[48:]) != 0xcd80c728 { // checksum
		t.Fatal(binary.BigEndian.Uint32(buf.buf[48:]))
	}
}
