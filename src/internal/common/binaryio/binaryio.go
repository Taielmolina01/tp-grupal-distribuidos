package binaryio

import (
	"errors"

	"tp-grupal-distribuidos/internal/common/byteconv"
)

type Writer struct {
	buf []byte
}

func (w *Writer) Int32(v int32)   { w.buf = byteconv.AppendUint32(w.buf, uint32(v)) }
func (w *Writer) Uint32(v uint32) { w.buf = byteconv.AppendUint32(w.buf, v) }
func (w *Writer) Uint64(v uint64) { w.buf = byteconv.AppendUint64(w.buf, v) }
func (w *Writer) Bytes() []byte   { return w.buf }

type Reader struct {
	data []byte
	pos  int
	err  error
}

func NewReader(data []byte) *Reader { return &Reader{data: data} }

func (r *Reader) Remaining() int { return len(r.data) - r.pos }
func (r *Reader) Err() error     { return r.err }

func (r *Reader) Int32() int32   { return int32(r.Uint32()) }
func (r *Reader) Uint32() uint32 {
	if r.err != nil {
		return 0
	}
	if r.pos+4 > len(r.data) {
		r.err = errors.New("binaryio: unexpected EOF reading uint32")
		return 0
	}
	v := uint32(r.data[r.pos])<<24 | uint32(r.data[r.pos+1])<<16 | uint32(r.data[r.pos+2])<<8 | uint32(r.data[r.pos+3])
	r.pos += 4
	return v
}

func (r *Reader) Uint64() uint64 {
	if r.err != nil {
		return 0
	}
	if r.pos+8 > len(r.data) {
		r.err = errors.New("binaryio: unexpected EOF reading uint64")
		return 0
	}
	v := uint64(r.data[r.pos])<<56 | uint64(r.data[r.pos+1])<<48 | uint64(r.data[r.pos+2])<<40 | uint64(r.data[r.pos+3])<<32 |
		uint64(r.data[r.pos+4])<<24 | uint64(r.data[r.pos+5])<<16 | uint64(r.data[r.pos+6])<<8 | uint64(r.data[r.pos+7])
	r.pos += 8
	return v
}
