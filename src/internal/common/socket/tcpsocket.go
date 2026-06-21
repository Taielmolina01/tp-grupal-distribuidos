package socket

import "net"

type TCPSocket interface {
	SendMessage(message []byte) error
	ReadMessage(length int) ([]byte, error)
	Close() error
}

type tcpSocket struct {
	conn net.Conn
}

func NewTCPSocket(conn net.Conn) TCPSocket {
	return &tcpSocket{
		conn: conn,
	}
}

func (s *tcpSocket) SendMessage(message []byte) error {
	sent := 0
	for sent < len(message) {
		n, err := s.conn.Write(message[sent:])
		if err != nil {
			return err
		}
		sent += n
	}
	return nil
}

func (s *tcpSocket) ReadMessage(length int) ([]byte, error) {
	buf := make([]byte, length)
	received := 0
	for received < length {
		n, err := s.conn.Read(buf[received:])
		if err != nil {
			return nil, err
		}
		received += n
	}
	return buf, nil
}

func (s *tcpSocket) Close() error {
	return s.conn.Close()
}
