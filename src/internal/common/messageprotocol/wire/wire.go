package wire

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"time"
)

const (
	Uint8Size  uint32 = 1
	Uint16Size uint32 = 2
	Uint32Size uint32 = 4
	Uint64Size uint32 = 8
	Int64Size  uint32 = 8
	BoolSize   uint32 = 1
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

type Writer struct {
	buf []byte
}

func NewWriter() *Writer { return &Writer{} }

func (w *Writer) Uint8(v uint8)   { w.buf = append(w.buf, v) }
func (w *Writer) Int32(v int32)   { w.buf = binary.BigEndian.AppendUint32(w.buf, uint32(v)) }
func (w *Writer) Uint16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *Writer) Uint32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *Writer) Uint64(v uint64) { w.buf = binary.BigEndian.AppendUint64(w.buf, v) }
func (w *Writer) Float64(v float64) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, math.Float64bits(v))
}
func (w *Writer) Bool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}
func (w *Writer) String(v string) {
	w.buf = binary.BigEndian.AppendUint16(w.buf, uint16(len(v)))
	w.buf = append(w.buf, v...)
}
func (w *Writer) Time(t time.Time) {
	w.buf = binary.BigEndian.AppendUint64(w.buf, uint64(t.Unix()))
}

func (w *Writer) Bytes() []byte     { return w.buf }
func (w *Writer) Len() int          { return len(w.buf) }
func (w *Writer) Truncate(n int)    { w.buf = w.buf[:n] }
func (w *Writer) Reset()            { w.buf = w.buf[:0] }
func (w *Writer) WriteRaw(b []byte) { w.buf = append(w.buf, b...) }

func NewWriterWithBytes(data []byte) *Writer {
	buf := make([]byte, len(data))
	copy(buf, data)
	return &Writer{buf: buf}
}

func AppendUint8(dst []byte, v uint8) []byte   { return append(dst, v) }
func AppendUint16(dst []byte, v uint16) []byte { return binary.BigEndian.AppendUint16(dst, v) }
func AppendUint32(dst []byte, v uint32) []byte { return binary.BigEndian.AppendUint32(dst, v) }
func AppendUint64(dst []byte, v uint64) []byte { return binary.BigEndian.AppendUint64(dst, v) }
func AppendInt64(dst []byte, v int64) []byte   { return binary.BigEndian.AppendUint64(dst, uint64(v)) }
func AppendFloat64(dst []byte, v float64) []byte {
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(v))
}
func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}
func AppendString(dst []byte, s string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

type Reader struct {
	data []byte
	pos  int
	err  error
}

func NewReader(data []byte) *Reader {
	return &Reader{data: data}
}

func (r *Reader) Remaining() int { return len(r.data) - r.pos }
func (r *Reader) Err() error     { return r.err }

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

func (r *Reader) Int32() int32 { return int32(r.Uint32()) }

func (r *Reader) Uint16() uint16 {
	b := r.read(2)
	if r.err != nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}

func (r *Reader) Uint32() uint32 {
	b := r.read(4)
	if r.err != nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (r *Reader) Uint64() uint64 {
	b := r.read(8)
	if r.err != nil {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func (r *Reader) Float64() float64 {
	return math.Float64frombits(r.Uint64())
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
	return time.Unix(int64(binary.BigEndian.Uint64(b)), 0).UTC()
}

func (r *Reader) ReadRaw(n uint64) []byte {
	return r.read(int(n))
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
