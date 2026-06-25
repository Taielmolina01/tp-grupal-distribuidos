package checkpoint

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

type Checkpoint[S any] struct {
	dir         string
	marshalFn   func(*S) []byte
	unmarshalFn func([]byte) (*S, error)
}

func New[S any](dir string, marshal func(*S) []byte, unmarshal func([]byte) (*S, error)) (*Checkpoint[S], error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("checkpoint: create dir %s: %w", dir, err)
	}
	return &Checkpoint[S]{dir: dir, marshalFn: marshal, unmarshalFn: unmarshal}, nil
}

func (c *Checkpoint[S]) SaveClient(clientID int, state *S) error {
	data := c.marshalFn(state)
	path := c.clientPath(clientID)
	tmp := path + ".tmp"

	f, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("checkpoint: create tmp client %d: %w", clientID, err)
	}
	_, writeErr := f.Write(data)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil {
		return fmt.Errorf("checkpoint: write client %d: %w", clientID, writeErr)
	}
	if syncErr != nil {
		return fmt.Errorf("checkpoint: sync client %d: %w", clientID, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("checkpoint: close client %d: %w", clientID, closeErr)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("checkpoint: rename client %d: %w", clientID, err)
	}
	return nil
}

func (c *Checkpoint[S]) DeleteClient(clientID int) error {
	path := c.clientPath(clientID)
	if err := os.Remove(path); err != nil {
		slog.Error("checkpoint: delete client", "clientID", clientID, "err", err)
		if os.IsNotExist(err) {
			return nil
		}
		if errors.Is(err, syscall.EBUSY) {
			return err
		}
	}
	return nil
}

func (c *Checkpoint[S]) Load() (map[int]*S, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: read dir %s: %w", c.dir, err)
	}

	states := make(map[int]*S, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".bin") {
			continue
		}
		clientID, err := strconv.Atoi(strings.TrimSuffix(name, ".bin"))
		if err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join(c.dir, name))
		if err != nil {
			return nil, fmt.Errorf("checkpoint: read client %d: %w", clientID, err)
		}
		state, err := c.unmarshalFn(data)
		if err != nil {
			return nil, fmt.Errorf("checkpoint: unmarshal client %d: %w", clientID, err)
		}
		states[clientID] = state
	}
	return states, nil
}

func (c *Checkpoint[S]) clientPath(clientID int) string {
	return filepath.Join(c.dir, strconv.Itoa(clientID)+".bin")
}
