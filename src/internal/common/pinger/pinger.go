package pinger

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"tp-grupal-distribuidos/internal/common/cleanup"
)

const (
	Ping byte = 1
	Pong byte = 2

	DefaultHealthPort = "8001"
	DefaultBullyPort  = "8002"
	DefaultPort       = DefaultHealthPort
)

type Pinger struct {
	conn *net.UDPConn
}

func New(addr string) (*Pinger, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return &Pinger{conn: conn}, nil
}

func (p *Pinger) Run() {
	slog.Info("Pinger listening", "addr", p.conn.LocalAddr().String())

	buf := make([]byte, 1)
	for {
		n, peer, err := p.conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			slog.Error("While reading ping", "err", err)
			continue
		}

		if n == 0 || buf[0] != Ping {
			continue
		}

		if _, err := p.conn.WriteToUDP([]byte{Pong}, peer); err != nil {
			slog.Error("While replying pong", "peer", peer.String(), "err", err)
		}
	}
}

func Check(addr string, timeout time.Duration) error {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("While closing connection", "err", err)
		}
	}()

	if _, err := conn.Write([]byte{Ping}); err != nil {
		return err
	}

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}

	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	if n != 1 || buf[0] != Pong {
		return fmt.Errorf("unexpected reply from %s: %v", addr, buf[:n])
	}
	return nil
}

func (p *Pinger) Close() {
	cleanup.Close(p.conn)
}

func Serve(addr string) *Pinger {
	p, err := New(addr)
	if err != nil {
		slog.Error("While starting pinger", "addr", addr, "err", err)
		os.Exit(1)
	}
	go p.Run()
	return p
}
