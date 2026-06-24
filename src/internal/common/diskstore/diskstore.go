package diskstore

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"tp-grupal-distribuidos/internal/common/messageprotocol/safeio"
)

func WriteTmp(path string, data map[string][]byte) error {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return fmt.Errorf("diskstore: create tmp: %w", err)
	}
	defer func() { _ = f.Close() }()

	if err := writeMap(f, data); err != nil {
		return err
	}
	return f.Sync()
}

func Commit(path string) error {
	return os.Rename(path+".tmp", path)
}

func WriteAtomic(path string, data map[string][]byte) error {
	if err := WriteTmp(path, data); err != nil {
		return err
	}
	return Commit(path)
}

func RemoveIfExists(path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func Read(path string) (map[string][]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("diskstore: open: %w", err)
	}
	defer func() { _ = f.Close() }()
	return readMap(f)
}

func writeMap(w io.Writer, data map[string][]byte) error {
	hdr := binary.BigEndian.AppendUint32(nil, uint32(len(data)))
	if err := safeio.WriteAll(w, hdr); err != nil {
		return fmt.Errorf("diskstore: write count: %w", err)
	}
	for k, v := range data {
		keyLen := binary.BigEndian.AppendUint16(nil, uint16(len(k)))
		if err := safeio.WriteAll(w, keyLen); err != nil {
			return fmt.Errorf("diskstore: write key len: %w", err)
		}
		if err := safeio.WriteAll(w, []byte(k)); err != nil {
			return fmt.Errorf("diskstore: write key: %w", err)
		}
		valLen := binary.BigEndian.AppendUint32(nil, uint32(len(v)))
		if err := safeio.WriteAll(w, valLen); err != nil {
			return fmt.Errorf("diskstore: write val len: %w", err)
		}
		if err := safeio.WriteAll(w, v); err != nil {
			return fmt.Errorf("diskstore: write val: %w", err)
		}
	}
	return nil
}

func readMap(r io.Reader) (map[string][]byte, error) {
	hdr, err := safeio.ReadAll(r, 4)
	if err != nil {
		return nil, fmt.Errorf("diskstore: read count: %w", err)
	}
	count := binary.BigEndian.Uint32(hdr)

	m := make(map[string][]byte, count)
	for range count {
		keyLenBuf, err := safeio.ReadAll(r, 2)
		if err != nil {
			return nil, fmt.Errorf("diskstore: read key len: %w", err)
		}
		key, err := safeio.ReadAll(r, uint32(binary.BigEndian.Uint16(keyLenBuf)))
		if err != nil {
			return nil, fmt.Errorf("diskstore: read key: %w", err)
		}
		valLenBuf, err := safeio.ReadAll(r, 4)
		if err != nil {
			return nil, fmt.Errorf("diskstore: read val len: %w", err)
		}
		val, err := safeio.ReadAll(r, binary.BigEndian.Uint32(valLenBuf))
		if err != nil {
			return nil, fmt.Errorf("diskstore: read val: %w", err)
		}
		m[string(key)] = val
	}
	return m, nil
}
