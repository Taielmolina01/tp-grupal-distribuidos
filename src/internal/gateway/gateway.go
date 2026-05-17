package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"tp-grupal-distribuidos/internal/clientregistry"
	"tp-grupal-distribuidos/internal/common/messageprotocol/external"
	"tp-grupal-distribuidos/internal/common/middleware"
	"tp-grupal-distribuidos/internal/messagehandler"
)

type GatewayConfig struct {
	InputQueueName  string
	OutputQueueName string
	ServerHost      string
	ServerPort      string
	MomHost         string
	MomPort         int
}

type Gateway struct {
	registry    clientregistry.ClientRegistry
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	listener    net.Listener
	running     atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("While closing input queue", "err", err)
		}
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		if err := inputQueue.Close(); err != nil {
			slog.Error("While closing input queue", "err", err)
		}
		if err := outputQueue.Close(); err != nil {
			slog.Error("While closing input queue", "err", err)
		}
		return nil, err
	}

	gateway := &Gateway{outputQueue: outputQueue, inputQueue: inputQueue, listener: listener}
	gateway.running.Store(true)
	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer func() {
		if err := gateway.listener.Close(); err != nil {
			slog.Error("While closing gateway's acceptor socket", "err", err)
		}
	}()

	go func() {
		if err := gateway.outputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			gateway.handleClientResponse(msg, ack, nack)
		}); err != nil {
			slog.Error("While consuming output queue", "err", err)
		}
	}()
	go gateway.handleSignals()

	slog.Info("Accepting connections...")

	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}
			return err
		}

		slog.Info("Client connected...")

		handler := messagehandler.NewMessageHandler()
		client := clientregistry.ClientState{Conn: conn, Handler: &handler}

		gateway.registry.Add(client)

		go gateway.handleClientRequest(client)
	}

	if err := gateway.outputQueue.StopConsuming(); err != nil {
		slog.Error("While stopping output queue", "err", err)
	}
	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for _, client := range clients {
			if err := client.Conn.Close(); err != nil {
				slog.Error("While closing client's socket", "err", err)
			}
		}
	})
	return nil
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	gateway.running.Store(false)
	if err := gateway.listener.Close(); err != nil {
		slog.Error("While closing acceptor socket", "err", err)
	}
}

func (gateway *Gateway) handleClientRequest(client clientregistry.ClientState) {
loop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Debug("While reading message type", "err", err)
			return
		}

		switch msgType {
		case external.FruitRecord:
			if err := gateway.handleFruitRecordMessage(client); err != nil {
				slog.Debug("While handling record message", "err", err)
				return
			}

		case external.EndOfRecords:
			if err := gateway.handleEndOfRecordsMessage(client); err != nil {
				slog.Debug("While handling end of records message", "err", err)
				return
			}
			break loop

		default:
			slog.Debug("Read unexpected message type")
			return
		}
	}
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {

	clientIndex := -1

	gateway.registry.WithLock(func(clients []clientregistry.ClientState) {
		for i, client := range clients {
			fruitTop, err := client.Handler.DeserializeResultMessage(&msg)

			if err != nil {
				slog.Debug("While reading from output queue", "err", err)
				nack()
				if err := gateway.outputQueue.StopConsuming(); err != nil {
					slog.Error("While stopping output queue", "err", err)
				}
				return
			}

			// The message handler can't process this message
			if fruitTop == nil {
				continue
			}

			if err := external.WriteFruitTop(client.Conn, fruitTop); err != nil {
				slog.Debug("While writing FRUIT_TOP message", "err", err)
				return
			}
			msgType, err := external.ReadMsgType(client.Conn)
			if err != nil {
				slog.Debug("While reading message type", "err", err)
				return
			}
			if msgType != external.Ack {
				slog.Debug("Expected ACK message")
				return
			}

			clientIndex = i
			return
		}
		slog.Warn("No client handler could process this message")
		nack()
	})

	if clientIndex >= 0 {
		gateway.registry.Remove(clientIndex)
		ack()
		return
	}
}

func (gateway *Gateway) handleFruitRecordMessage(client clientregistry.ClientState) error {
	fruitRecord, err := external.ReadFruitRecord(client.Conn)
	if err != nil {
		slog.Debug("While reading FRUIT_RECORD", "err", err)
		return err
	}
	message, err := client.Handler.SerializeDataMessage(*fruitRecord)
	if err != nil {
		slog.Debug("While serializing data message", "err", err)
		return err
	}
	if err := gateway.inputQueue.Send(*message); err != nil {
		slog.Debug("While sending data message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleEndOfRecordsMessage(client clientregistry.ClientState) error {
	slog.Info("Received END_OF_RECORDS message")
	message, err := client.Handler.SerializeEOFMessage()
	if err != nil {
		slog.Debug("While serializing END_OF_RECORDS  message", "err", err)
		return err
	}
	if err := gateway.inputQueue.Send(*message); err != nil {
		slog.Debug("While sending eof message", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}
