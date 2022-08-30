package main

import (
	"fmt"
	"io"
)

type memory []byte

// Open creates a type that can use a Reader/Writer interface to the
// underlying byte array.
func (base memory) Open() *RamIO {
	return &RamIO{
		data: base,
		max:  cap(base),
	}
}

// RamIO implements various io interfaces, using an underlying byte array.
type RamIO struct {
	data    []byte
	current int
	max     int
}

// Write copies the byte slice into the RAM array
func (r *RamIO) Write(p []byte) (int, error) {
	n := copy(r.data[r.current:], p)
	r.current += n
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

// WriteAt copies the byte slice into the RAM array at the offset specified
func (r *RamIO) WriteAt(p []byte, offs int64) (int, error) {
	if int(offs) >= r.max {
		return 0, io.EOF
	}
	r.current = int(offs)
	return r.Write(p)
}

func (r *RamIO) WriteByte(b byte) error {
	if r.current >= r.max {
		return io.EOF
	}
	r.data[r.current] = b
	r.current++
	return nil
}

// Seek moves the offset
func (r *RamIO) Seek(offs int64, whence int) (int64, error) {
	n := int(offs)
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		n += r.current
	case io.SeekEnd:
		n = r.max - n
	default:
		return 0, fmt.Errorf("unknown whence")
	}
	if n < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	r.current = n
	return int64(r.current), nil
}

func (r *RamIO) ReadByte() (byte, error) {
	if r.current >= r.max {
		return 0, io.EOF
	}
	b := r.data[r.current]
	r.current++
	return b, nil
}

func (r *RamIO) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.current:])
	r.current += n
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *RamIO) ReadAt(p []byte, offs int64) (int, error) {
	if int(offs) >= r.max {
		return 0, io.EOF
	}
	r.current = int(offs)
	return r.Read(p)
}
