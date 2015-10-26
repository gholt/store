package valuestore

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"
)

func TestValueValuesFileReading(t *testing.T) {
	store, err := NewValueStore(lowMemValueStoreConfig())
	if err != nil {
		t.Fatal("")
	}
	buf := &memBuf{buf: []byte("0123456789abcdef")}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	fl, err := newValueFile(store, 12345, openReadSeeker)
	if err != nil {
		t.Fatal("")
	}
	if fl == nil {
		t.Fatal("")
	}
	tsn := fl.timestampnano()
	if tsn != 12345 {
		t.Fatal(tsn)
	}
	ts, v, err := fl.read(1, 2, 0x300, 4, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "45678" {
		t.Fatal(string(v))
	}
	ts, v, err = fl.read(1, 2, 0x300|_TSB_DELETION, 4, 5, nil)
	if err != ErrNotFound {
		t.Fatal(err)
	}
	if ts != 0x300|_TSB_DELETION {
		t.Fatal(ts)
	}
	if v != nil {
		t.Fatal(v)
	}
	ts, v, err = fl.read(1, 2, 0x300, 4, 5, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "45678" {
		t.Fatal(string(v))
	}
	_, _, err = fl.read(1, 2, 0x300, 12, 5, nil)
	if err != io.EOF {
		t.Fatal(err)
	}
	ts, v, err = fl.read(1, 2, 0x300, 4, 5, []byte("testing"))
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
	ts, v, err = fl.read(1, 2, 0x300, 4, 5, v)
	if err != nil {
		t.Fatal(err)
	}
	if ts != 0x300 {
		t.Fatal(ts)
	}
	if string(v) != "45678" {
		t.Fatal(string(v))
	}
	ts, v, err = fl.read(1, 2, 0x300, 4, 5, v)
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

func TestValueValuesFileWritingEmpty(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.ChecksumInterval = 64*1024 - 4
	store, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	buf := &memBuf{}
	createWriteCloser := func(name string) (io.WriteCloser, error) {
		return &memFile{buf: buf}, nil
	}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	fl, err := createValueFile(store, createWriteCloser, openReadSeeker)
	if err != nil {
		t.Fatal("")
	}
	if fl == nil {
		t.Fatal("")
	}
	fl.close()
	bl := len(buf.buf)
	if bl != 52 {
		t.Fatal(bl)
	}
	if string(buf.buf[:28]) != "VALUESTORE v0               " {
		t.Fatal(string(buf.buf[:28]))
	}
	if binary.BigEndian.Uint32(buf.buf[28:]) != store.checksumInterval {
		t.Fatal(binary.BigEndian.Uint32(buf.buf[28:]), store.checksumInterval)
	}
	if binary.BigEndian.Uint32(buf.buf[bl-20:]) != 0 { // unused at this time
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-20:]))
	}
	if binary.BigEndian.Uint64(buf.buf[bl-16:]) != 32 { // last offset, 0 past header
		t.Fatal(binary.BigEndian.Uint64(buf.buf[bl-16:]))
	}
	if string(buf.buf[bl-8:bl-4]) != "TERM" {
		t.Fatal(string(buf.buf[bl-8 : bl-4]))
	}
	if binary.BigEndian.Uint32(buf.buf[bl-4:]) != 0xcd80c728 { // checksum
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-4:]))
	}
}

func TestValueValuesFileWritingEmpty2(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.ChecksumInterval = 64*1024 - 4
	store, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	store.freeableMemBlockChans = make([]chan *valueMemBlock, 1)
	store.freeableMemBlockChans[0] = make(chan *valueMemBlock, 1)
	buf := &memBuf{}
	createWriteCloser := func(name string) (io.WriteCloser, error) {
		return &memFile{buf: buf}, nil
	}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	fl, err := createValueFile(store, createWriteCloser, openReadSeeker)
	if err != nil {
		t.Fatal("")
	}
	if fl == nil {
		t.Fatal("")
	}
	memBlock := &valueMemBlock{values: []byte{}}
	memBlock.fileID = 123
	fl.write(memBlock)
	fl.close()
	if memBlock.fileID != fl.id {
		t.Fatal(memBlock.fileID, fl.id)
	}
	bl := len(buf.buf)
	if bl != 52 {
		t.Fatal(bl)
	}
	if string(buf.buf[:28]) != "VALUESTORE v0               " {
		t.Fatal(string(buf.buf[:28]))
	}
	if binary.BigEndian.Uint32(buf.buf[28:]) != store.checksumInterval {
		t.Fatal(binary.BigEndian.Uint32(buf.buf[28:]), store.checksumInterval)
	}
	if binary.BigEndian.Uint32(buf.buf[bl-20:]) != 0 { // unused at this time
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-20:]))
	}
	if binary.BigEndian.Uint64(buf.buf[bl-16:]) != 32 { // last offset
		t.Fatal(binary.BigEndian.Uint64(buf.buf[bl-16:]))
	}
	if string(buf.buf[bl-8:bl-4]) != "TERM" {
		t.Fatal(string(buf.buf[bl-8 : bl-4]))
	}
	if binary.BigEndian.Uint32(buf.buf[bl-4:]) != 0xcd80c728 { // checksum
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-4:]))
	}
}

func TestValueValuesFileWriting(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.ChecksumInterval = 64*1024 - 4
	store, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	buf := &memBuf{}
	createWriteCloser := func(name string) (io.WriteCloser, error) {
		return &memFile{buf: buf}, nil
	}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	fl, err := createValueFile(store, createWriteCloser, openReadSeeker)
	if err != nil {
		t.Fatal("")
	}
	if fl == nil {
		t.Fatal("")
	}
	values := make([]byte, 1234)
	copy(values, []byte("0123456789abcdef"))
	values[1233] = 1
	fl.write(&valueMemBlock{values: values})
	fl.close()
	bl := len(buf.buf)
	if bl != 1234+52 {
		t.Fatal(bl)
	}
	if string(buf.buf[:28]) != "VALUESTORE v0               " {
		t.Fatal(string(buf.buf[:28]))
	}
	if binary.BigEndian.Uint32(buf.buf[28:]) != store.checksumInterval {
		t.Fatal(binary.BigEndian.Uint32(buf.buf[28:]), store.checksumInterval)
	}
	if !bytes.Equal(buf.buf[32:bl-20], values) {
		t.Fatal("")
	}
	if binary.BigEndian.Uint32(buf.buf[bl-20:]) != 0 { // unused at this time
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-20:]))
	}
	if binary.BigEndian.Uint64(buf.buf[bl-16:]) != 1234+32 { // last offset
		t.Fatal(binary.BigEndian.Uint64(buf.buf[bl-16:]))
	}
	if string(buf.buf[bl-8:bl-4]) != "TERM" {
		t.Fatal(string(buf.buf[bl-8 : bl-4]))
	}
	if binary.BigEndian.Uint32(buf.buf[bl-4:]) != 0x941edfb6 { // checksum
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-4:]))
	}
}

func TestValueValuesFileWritingMore(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.ChecksumInterval = 64*1024 - 4
	store, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	buf := &memBuf{}
	createWriteCloser := func(name string) (io.WriteCloser, error) {
		return &memFile{buf: buf}, nil
	}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	fl, err := createValueFile(store, createWriteCloser, openReadSeeker)
	if err != nil {
		t.Fatal("")
	}
	if fl == nil {
		t.Fatal("")
	}
	values := make([]byte, 123456)
	copy(values, []byte("0123456789abcdef"))
	values[1233] = 1
	fl.write(&valueMemBlock{values: values})
	fl.close()
	bl := len(buf.buf)
	if bl != 123456+int(123512/store.checksumInterval*4)+52 {
		t.Fatal(bl)
	}
	if string(buf.buf[:28]) != "VALUESTORE v0               " {
		t.Fatal(string(buf.buf[:28]))
	}
	if binary.BigEndian.Uint32(buf.buf[28:]) != store.checksumInterval {
		t.Fatal(binary.BigEndian.Uint32(buf.buf[28:]), store.checksumInterval)
	}
	if binary.BigEndian.Uint32(buf.buf[bl-20:]) != 0 { // unused at this time
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-20:]))
	}
	if binary.BigEndian.Uint64(buf.buf[bl-16:]) != 123456+32 { // last offset
		t.Fatal(binary.BigEndian.Uint64(buf.buf[bl-16:]))
	}
	if string(buf.buf[bl-8:bl-4]) != "TERM" {
		t.Fatal(string(buf.buf[bl-8 : bl-4]))
	}
	if binary.BigEndian.Uint32(buf.buf[bl-4:]) != 0x6aa30474 { // checksum
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-4:]))
	}
}

func TestValueValuesFileWritingMultiple(t *testing.T) {
	cfg := lowMemValueStoreConfig()
	cfg.ChecksumInterval = 64*1024 - 4
	store, err := NewValueStore(cfg)
	if err != nil {
		t.Fatal("")
	}
	store.freeableMemBlockChans = make([]chan *valueMemBlock, 1)
	store.freeableMemBlockChans[0] = make(chan *valueMemBlock, 2)
	buf := &memBuf{}
	createWriteCloser := func(name string) (io.WriteCloser, error) {
		return &memFile{buf: buf}, nil
	}
	openReadSeeker := func(name string) (io.ReadSeeker, error) {
		return &memFile{buf: buf}, nil
	}
	fl, err := createValueFile(store, createWriteCloser, openReadSeeker)
	if err != nil {
		t.Fatal("")
	}
	if fl == nil {
		t.Fatal("")
	}
	values1 := make([]byte, 12345)
	copy(values1, []byte("0123456789abcdef"))
	memBlock1 := &valueMemBlock{values: values1}
	fl.write(memBlock1)
	values2 := make([]byte, 54321)
	copy(values2, []byte("fedcba9876543210"))
	memBlock2 := &valueMemBlock{values: values2}
	fl.write(memBlock2)
	fl.close()
	if memBlock1.fileID != fl.id {
		t.Fatal(memBlock1.fileID, fl.id)
	}
	if memBlock2.fileID != fl.id {
		t.Fatal(memBlock2.fileID, fl.id)
	}
	bl := len(buf.buf)
	if bl != 12345+54321+int(123512/store.checksumInterval*4)+52 {
		t.Fatal(bl)
	}
	if string(buf.buf[:28]) != "VALUESTORE v0               " {
		t.Fatal(string(buf.buf[:28]))
	}
	if binary.BigEndian.Uint32(buf.buf[28:]) != store.checksumInterval {
		t.Fatal(binary.BigEndian.Uint32(buf.buf[28:]), store.checksumInterval)
	}
	if binary.BigEndian.Uint32(buf.buf[bl-20:]) != 0 { // unused at this time
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-20:]))
	}
	if binary.BigEndian.Uint64(buf.buf[bl-16:]) != 12345+54321+32 { // last offset
		t.Fatal(binary.BigEndian.Uint64(buf.buf[bl-16:]))
	}
	if string(buf.buf[bl-8:bl-4]) != "TERM" {
		t.Fatal(string(buf.buf[bl-8 : bl-4]))
	}
	if binary.BigEndian.Uint32(buf.buf[bl-4:]) != 0xacac4386 { // checksum
		t.Fatal(binary.BigEndian.Uint32(buf.buf[bl-4:]))
	}
}
