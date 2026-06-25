package gateway

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type bufferEntry struct {
	mu        sync.Mutex
	recovered bool
}

type resultBuffer struct {
	dir     string
	mu      sync.Mutex
	clients map[int]*bufferEntry
}

func newResultBuffer(dir string) (*resultBuffer, error) {
	if dir != "" {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}
	return &resultBuffer{dir: dir, clients: map[int]*bufferEntry{}}, nil
}

func (b *resultBuffer) entry(clientID int) *bufferEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.clients[clientID]
	if !ok {
		e = &bufferEntry{}
		b.clients[clientID] = e
	}
	return e
}

func (b *resultBuffer) lock(clientID int) *sync.Mutex {
	return &b.entry(clientID).mu
}

func (b *resultBuffer) path(clientID int) string {
	return filepath.Join(b.dir, "results_"+strconv.Itoa(clientID)+".buf")
}

func (b *resultBuffer) recoverOnce(clientID int) error {
	e := b.entry(clientID)
	if e.recovered {
		return nil
	}
	if err := b.truncateTorn(clientID); err != nil {
		return err
	}
	e.recovered = true
	return nil
}

func (b *resultBuffer) truncateTorn(clientID int) error {
	f, err := os.OpenFile(b.path(clientID), os.O_RDWR, 0644)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); err != nil {
			slog.Debug("While closing result file", "err", err)
		}
	}()

	var offset int64
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			break
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[:]))
		if _, err := io.ReadFull(f, body); err != nil {
			break
		}
		offset += 4 + int64(len(body))
	}
	return f.Truncate(offset)
}

func (b *resultBuffer) append(clientID int, body []byte) error {
	if err := b.recoverOnce(clientID); err != nil {
		return err
	}

	f, err := os.OpenFile(b.path(clientID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := f.Write(hdr[:]); err != nil {
		if err := f.Close(); err != nil {
			slog.Debug("While closing result file after write error", "err", err)
		}
		return err
	}
	if _, err := f.Write(body); err != nil {
		if err := f.Close(); err != nil {
			slog.Debug("While closing result file after write error", "err", err)
		}
		return err
	}
	if err := f.Sync(); err != nil {
		if err := f.Close(); err != nil {
			slog.Debug("While closing result file after sync error", "err", err)
		}
		return err
	}
	return f.Close()
}

func (b *resultBuffer) flush(clientID int, send func(body []byte) error) error {
	if err := b.recoverOnce(clientID); err != nil {
		return err
	}

	path := b.path(clientID)
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	for {
		var hdr [4]byte
		if _, err := io.ReadFull(f, hdr[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if err := f.Close(); err != nil {
				slog.Debug("While closing result file after read error", "err", err)
			}
			return err
		}
		body := make([]byte, binary.BigEndian.Uint32(hdr[:]))
		if _, err := io.ReadFull(f, body); err != nil {
			if err := f.Close(); err != nil {
				slog.Debug("While closing result file after read error", "err", err)
			}
			return err
		}
		if err := send(body); err != nil {
			if err := f.Close(); err != nil {
				slog.Debug("While closing result file after send error", "err", err)
			}
			return err
		}
	}

	if err := f.Close(); err != nil {
		slog.Debug("While closing result file", "err", err)
	}
	return os.Remove(path)
}

func (b *resultBuffer) remove(clientID int) error {
	err := os.Remove(b.path(clientID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
