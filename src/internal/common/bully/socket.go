package bully

import "net"

type UDPSocket struct {
	socket net.PacketConn
}

func CreateUDPSocket(port int) (*UDPSocket, error) {
	addr := net.UDPAddr{
		Port: port,
		IP:   nil,
	}
	skt, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return nil, err
	}
	return &UDPSocket{socket: skt}, nil
}

func (s *UDPSocket) SendMessage(message []byte, addr net.UDPAddr) error {
	sent := 0
	for sent < len(message) {
		n, err := s.socket.WriteTo(message[sent:], &addr)
		if err != nil {
			return err
		}
		sent += n
	}
	return nil
}

func (s *UDPSocket) ReceiveMessage(buffer []byte) (int, net.UDPAddr, error) {
	received := 0
	var address *net.UDPAddr
	for received < len(buffer) {
		n, addr, err := s.socket.ReadFrom(buffer[received:])
		if err != nil {
			return 0, net.UDPAddr{}, err
		}
		received += n
		address = addr.(*net.UDPAddr)
	}
	return len(buffer), *address, nil
}

func (s *UDPSocket) Close() error {
	return s.socket.Close()
}
