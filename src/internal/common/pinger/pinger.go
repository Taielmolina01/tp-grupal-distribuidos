package pinger

import (
	"errors"
	"log/slog"
	"net"
	"os"
)

const (
	Ping byte = 1
	Pong byte = 2

	DefaultPort = "8001"
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

func (p *Pinger) Close() {
	if err := p.conn.Close(); err != nil {
		slog.Error("While closing pinger", "err", err)
	}
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
