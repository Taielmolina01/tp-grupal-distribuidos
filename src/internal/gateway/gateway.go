package gateway

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/clientregistry"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/messageprotocol/external"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/common/middleware"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/internal/messagehandler"
)

type GatewayConfig struct {
	AccountsExchange  string
	TransfersExchange string
	ResultsQueue      string
	ServerHost        string
	ServerPort        string
	MomHost           string
	MomPort           int
}

type Gateway struct {
	registry          clientregistry.ClientRegistry
	accountsExchange  middleware.Middleware
	transfersExchange middleware.Middleware
	resultsQueue      middleware.Middleware
	listener          net.Listener
	running           atomic.Bool
}

func NewGateway(config GatewayConfig) (*Gateway, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	accountsExchange, err := middleware.CreateExchangeMiddleware(config.AccountsExchange, []string{}, connSettings)
	if err != nil {
		return nil, err
	}

	transfersExchange, err := middleware.CreateExchangeMiddleware(config.TransfersExchange, []string{}, connSettings)
	if err != nil {
		if err := accountsExchange.Close(); err != nil {
			slog.Error("While closing accounts exchange", "err", err)
		}
		return nil, err
	}

	resultsQueue, err := middleware.CreateQueueMiddleware(config.ResultsQueue, connSettings)
	if err != nil {
		if err := accountsExchange.Close(); err != nil {
			slog.Error("While closing accounts exchange", "err", err)
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		if err := accountsExchange.Close(); err != nil {
			slog.Error("While closing accounts exchange", "err", err)
		}
		if err := transfersExchange.Close(); err != nil {
			slog.Error("While closing transfers exchange", "err", err)
		}
		if err := resultsQueue.Close(); err != nil {
			slog.Error("While closing results queue", "err", err)
		}
		return nil, err
	}

	gateway := &Gateway{
		accountsExchange:  accountsExchange,
		transfersExchange: transfersExchange,
		resultsQueue:      resultsQueue,
		listener:          listener,
	}
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
		if err := gateway.resultsQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			gateway.handleClientResponse(msg, ack, nack)
		}); err != nil {
			slog.Error("While consuming results queue", "err", err)
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

	if err := gateway.resultsQueue.StopConsuming(); err != nil {
		slog.Error("While stopping results queue consumer", "err", err)
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
accountsLoop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Debug("While reading message type (accounts phase)", "err", err)
			return
		}

		switch msgType {
		case external.AccountRecord:
			if err := gateway.handleAccountMessage(client); err != nil {
				slog.Debug("While handling account message", "err", err)
				return
			}

		case external.EOFAccounts:
			if err := gateway.handleEOFAccountsMessage(client); err != nil {
				slog.Debug("While handling EOF accounts message", "err", err)
				return
			}
			break accountsLoop

		default:
			slog.Debug("Unexpected message type in accounts phase", "got", msgType)
			return
		}
	}

transfersLoop:
	for {
		msgType, err := external.ReadMsgType(client.Conn)
		if err != nil {
			slog.Debug("While reading message type (transfers phase)", "err", err)
			return
		}

		switch msgType {
		case external.TransferRecord:
			if err := gateway.handleTransferMessage(client); err != nil {
				slog.Debug("While handling transfer message", "err", err)
				return
			}

		case external.EOFTransfers:
			if err := gateway.handleEOFTransfersMessage(client); err != nil {
				slog.Debug("While handling EOF transfers message", "err", err)
				return
			}
			break transfersLoop

		default:
			slog.Debug("Unexpected message type in transfers phase", "got", msgType)
			return
		}
	}
}

func (gateway *Gateway) handleClientResponse(msg middleware.Message, ack func(), nack func()) {
	slog.Info("Received result from pipeline", "body", msg.Body)
	ack()
}

func (gateway *Gateway) forwardRecord(client clientregistry.ClientState, record any, exchange middleware.Middleware) error {
	body, err := json.Marshal(record)
	if err != nil {
		slog.Debug("While serializing record", "err", err)
		return err
	}
	if err := exchange.Send(middleware.Message{Body: string(body)}); err != nil {
		slog.Debug("While sending record to exchange", "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) forwardEOF(client clientregistry.ClientState, kind string, exchange middleware.Middleware) error {
	slog.Info("Received EOF message", "kind", kind)
	body := fmt.Sprintf(`{"eof":%q}`, kind)
	if err := exchange.Send(middleware.Message{Body: body}); err != nil {
		slog.Debug("While sending EOF", "kind", kind, "err", err)
		return err
	}
	if err := external.WriteAck(client.Conn); err != nil {
		slog.Debug("While writing ACK message", "err", err)
		return err
	}
	return nil
}

func (gateway *Gateway) handleAccountMessage(client clientregistry.ClientState) error {
	account, err := external.ReadAccountRecord(client.Conn)
	if err != nil {
		slog.Debug("While reading ACCOUNT_RECORD", "err", err)
		return err
	}
	return gateway.forwardRecord(client, account, gateway.accountsExchange)
}

func (gateway *Gateway) handleTransferMessage(client clientregistry.ClientState) error {
	transfer, err := external.ReadTransferRecord(client.Conn)
	if err != nil {
		slog.Debug("While reading TRANSFER_RECORD", "err", err)
		return err
	}
	return gateway.forwardRecord(client, transfer, gateway.transfersExchange)
}

func (gateway *Gateway) handleEOFAccountsMessage(client clientregistry.ClientState) error {
	return gateway.forwardEOF(client, "accounts", gateway.accountsExchange)
}

func (gateway *Gateway) handleEOFTransfersMessage(client clientregistry.ClientState) error {
	return gateway.forwardEOF(client, "transfers", gateway.transfersExchange)
}
