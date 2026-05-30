package wire

import (
	"errors"
	"io"
	"math"
	"time"

	"tp-grupal-distribuidos/internal/common/byteconv"
)

var (
	ErrFieldTooLarge = errors.New("wire: field length exceeds remaining bytes")
	ErrBatchTooLarge = errors.New("wire: batch count exceeds remaining bytes")
)

type Codec[T any] struct {
	Marshal   func(*Writer, *T)
	Unmarshal func(*Reader) T
	MinSize   uint32
}

// Writer accumulates fields into a byte slice with zero per-field allocations.
type Writer struct {
	buf []byte
}

func NewWriter() *Writer { return &Writer{} }

func (w *Writer) Uint8(v uint8)     { w.buf = byteconv.AppendUint8(w.buf, v) }
func (w *Writer) Uint16(v uint16)   { w.buf = byteconv.AppendUint16(w.buf, v) }
func (w *Writer) Uint32(v uint32)   { w.buf = byteconv.AppendUint32(w.buf, v) }
func (w *Writer) Float64(v float64) { w.buf = byteconv.AppendFloat64(w.buf, v) }
func (w *Writer) String(v string)   { w.buf = byteconv.AppendString(w.buf, v) }
func (w *Writer) Bool(v bool)       { w.buf = byteconv.AppendBool(w.buf, v) }
func (w *Writer) Time(t time.Time)  { w.buf = byteconv.AppendInt64(w.buf, t.Unix()) }

func (w *Writer) Bytes() []byte    { return w.buf }
func (w *Writer) Len() int         { return len(w.buf) }
func (w *Writer) Truncate(n int)   { w.buf = w.buf[:n] }
func (w *Writer) Reset()           { w.buf = w.buf[:0] }

type Reader struct {
	data []byte
	pos  int
	err  error
}

func NewReader(data []byte) *Reader {
	return &Reader{data: data}
}

func (r *Reader) Remaining() int { return len(r.data) - r.pos }

func (r *Reader) Err() error { return r.err }

func (r *Reader) read(n int) []byte {
	if r.err != nil {
		return nil
	}
	if r.pos+n > len(r.data) {
		r.err = io.ErrUnexpectedEOF
		return nil
	}
	b := r.data[r.pos : r.pos+n]
	r.pos += n
	return b
}

func (r *Reader) Uint8() uint8 {
	b := r.read(1)
	if r.err != nil {
		return 0
	}
	return b[0]
}

func (r *Reader) Uint16() uint16 {
	b := r.read(2)
	if r.err != nil {
		return 0
	}
	return uint16(b[0])<<8 | uint16(b[1])
}

func (r *Reader) Uint32() uint32 {
	b := r.read(4)
	if r.err != nil {
		return 0
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func (r *Reader) Float64() float64 {
	b := r.read(8)
	if r.err != nil {
		return 0
	}
	bits := uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
	return math.Float64frombits(bits)
}

func (r *Reader) Bool() bool {
	b := r.read(1)
	if r.err != nil {
		return false
	}
	return b[0] != 0
}

func (r *Reader) String() string {
	n := r.Uint16()
	if r.err != nil {
		return ""
	}
	if int(n) > r.Remaining() {
		r.err = ErrFieldTooLarge
		return ""
	}
	b := r.read(int(n))
	if r.err != nil {
		return ""
	}
	return string(b)
}

func (r *Reader) Time() time.Time {
	b := r.read(8)
	if r.err != nil {
		return time.Time{}
	}
	v := int64(b[0])<<56 | int64(b[1])<<48 | int64(b[2])<<40 | int64(b[3])<<32 |
		int64(b[4])<<24 | int64(b[5])<<16 | int64(b[6])<<8 | int64(b[7])
	return time.Unix(v, 0).UTC()
}

func (r *Reader) Count(minRecordSize uint32) uint32 {
	n := r.Uint16()
	if r.err != nil {
		return 0
	}
	if minRecordSize > 0 && uint64(n)*uint64(minRecordSize) > uint64(r.Remaining()) {
		r.err = ErrBatchTooLarge
		return 0
	}
	return uint32(n)
}
