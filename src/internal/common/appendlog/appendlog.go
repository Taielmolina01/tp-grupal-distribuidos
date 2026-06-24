package appendlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"tp-grupal-distribuidos/internal/common/messageprotocol/wire"
	"tp-grupal-distribuidos/internal/common/sendertracker"
)

const (
	keyword uint32 = 0x52495441

	headerSize   = 4 + 1 + 8 + 4 + 4
	checksumSize = 4
)

var ErrCorruptEntry = errors.New("appendlog: corrupt entry")

type Entry[T any] struct {
	SenderID uint8
	Seq      uint64
	Records  []T
}

type Log[T any] struct {
	path  string
	file  *os.File
	codec wire.Codec[T]
}

func Open[T any](path string, codec wire.Codec[T]) (*Log[T], error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("appendlog: create dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("appendlog: open: %w", err)
	}

	log := &Log[T]{path: path, file: file, codec: codec}
	if err := log.Recover(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return log, nil
}

func (l *Log[T]) AppendAll(entries []Entry[T]) error {
	if len(entries) == 0 {
		return nil
	}
	var buf []byte
	for _, e := range entries {
		buf = append(buf, l.buildFrame(e.SenderID, e.Seq, e.Records)...)
	}
	if _, err := l.file.Write(buf); err != nil {
		return fmt.Errorf("appendlog: append all: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("appendlog: sync: %w", err)
	}
	return nil
}

func (l *Log[T]) buildFrame(senderID uint8, seq uint64, records []T) []byte {
	payload := l.marshalRecords(records)
	frame := make([]byte, 0, headerSize+len(payload)+checksumSize)
	frame = binary.BigEndian.AppendUint32(frame, keyword)
	frame = append(frame, senderID)
	frame = binary.BigEndian.AppendUint64(frame, seq)
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(records)))
	frame = binary.BigEndian.AppendUint32(frame, uint32(len(payload)))
	frame = append(frame, payload...)
	frame = binary.BigEndian.AppendUint32(frame, crc32.ChecksumIEEE(frame[4:]))
	return frame
}

func (l *Log[T]) Recover() error {
	offset, err := l.validPrefixLen()
	if err != nil {
		return err
	}
	if err := l.file.Truncate(offset); err != nil {
		return fmt.Errorf("appendlog: truncate: %w", err)
	}
	if _, err := l.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("appendlog: seek end: %w", err)
	}
	return nil
}

func (l *Log[T]) ReadAll(fn func(Entry[T]) error) error {
	return l.readAll(nil, fn)
}

func (l *Log[T]) ReadUnique(tracker *sendertracker.SenderTracker, fn func(Entry[T]) error) error {
	if tracker == nil {
		return errors.New("appendlog: nil sender tracker")
	}
	return l.readAll(tracker, fn)
}

func (l *Log[T]) Close() error {
	return l.file.Close()
}

func (l *Log[T]) marshalRecords(records []T) []byte {
	w := wire.NewWriter()
	for i := range records {
		l.codec.Marshal(w, &records[i])
	}
	return w.Bytes()
}

func (l *Log[T]) validPrefixLen() (int64, error) {
	file, err := os.Open(l.path)
	if err != nil {
		return 0, fmt.Errorf("appendlog: open for recover: %w", err)
	}
	defer func() { _ = file.Close() }()

	var offset int64
	for {
		entryOffset := offset
		_, next, err := l.readEntryAt(file, entryOffset)

		if errors.Is(err, io.EOF) || errors.Is(err, ErrCorruptEntry) {
			return offset, nil
		}

		if err != nil {
			return 0, err
		}
		offset = next
	}
}

func (l *Log[T]) readAll(tracker *sendertracker.SenderTracker, fn func(Entry[T]) error) error {
	file, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("appendlog: open for read: %w", err)
	}
	defer func() { _ = file.Close() }()

	var offset int64
	for {
		entry, next, err := l.readEntryAt(file, offset)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, ErrCorruptEntry) {
			return fmt.Errorf("%w at offset %d", ErrCorruptEntry, offset)
		}
		if err != nil {
			return err
		}

		if tracker != nil {
			senderID := int(entry.SenderID)
			if tracker.IsDuplicate(senderID, entry.Seq) {
				offset = next
				continue
			}
			if err := fn(entry); err != nil {
				return err
			}
			tracker.Claim(senderID, entry.Seq)
		} else if err := fn(entry); err != nil {
			return err
		}

		offset = next
	}
}

func (l *Log[T]) readEntryAt(file *os.File, offset int64) (Entry[T], int64, error) {
	header := make([]byte, headerSize)
	n, err := file.ReadAt(header, offset)
	if errors.Is(err, io.EOF) && n == 0 {
		return Entry[T]{}, offset, io.EOF
	}
	if err != nil && errors.Is(err, io.EOF) {
		return Entry[T]{}, offset, ErrCorruptEntry
	}
	if err != nil {
		return Entry[T]{}, offset, fmt.Errorf("appendlog: read header: %w", err)
	}
	if binary.BigEndian.Uint32(header[:4]) != keyword {
		return Entry[T]{}, offset, ErrCorruptEntry
	}

	senderID := header[4]
	seq := binary.BigEndian.Uint64(header[5:13])
	count := binary.BigEndian.Uint32(header[13:17])
	payloadLen := binary.BigEndian.Uint32(header[17:21])

	payload := make([]byte, payloadLen)
	if _, err := file.ReadAt(payload, offset+headerSize); err != nil {
		if errors.Is(err, io.EOF) {
			return Entry[T]{}, offset, ErrCorruptEntry
		}
		return Entry[T]{}, offset, fmt.Errorf("appendlog: read payload: %w", err)
	}

	checksumBytes := make([]byte, checksumSize)
	if _, err := file.ReadAt(checksumBytes, offset+headerSize+int64(payloadLen)); err != nil {
		if errors.Is(err, io.EOF) {
			return Entry[T]{}, offset, ErrCorruptEntry
		}
		return Entry[T]{}, offset, fmt.Errorf("appendlog: read checksum: %w", err)
	}
	expected := binary.BigEndian.Uint32(checksumBytes)
	if checksum(header[4:], payload) != expected {
		return Entry[T]{}, offset, ErrCorruptEntry
	}

	records, err := l.unmarshalRecords(payload, count)
	if err != nil {
		return Entry[T]{}, offset, ErrCorruptEntry
	}

	next := offset + headerSize + int64(payloadLen) + checksumSize
	return Entry[T]{SenderID: senderID, Seq: seq, Records: records}, next, nil
}

func (l *Log[T]) unmarshalRecords(payload []byte, count uint32) ([]T, error) {
	r := wire.NewReader(payload)
	records := make([]T, 0, count)
	for range count {
		records = append(records, l.codec.Unmarshal(r))
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	if r.Remaining() != 0 {
		return nil, ErrCorruptEntry
	}
	return records, nil
}

func checksum(headerWithoutMagic []byte, payload []byte) uint32 {
	h := crc32.NewIEEE()
	_, _ = h.Write(headerWithoutMagic)
	_, _ = h.Write(payload)
	return h.Sum32()
}
