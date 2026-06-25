package msgsend

import (
	"log/slog"
	"tp-grupal-distribuidos/internal/common/messageprotocol/rabbit/batch"
	"tp-grupal-distribuidos/internal/common/middleware/newmiddleware"
)

func SendEOF(mw newmiddleware.Middleware, rk string, clientID int, queryID uint8, senderID uint8, seq uint64, total uint32) error {
	body := batch.WriteEOF(clientID, queryID, senderID, seq, total)
	return mw.Send(newmiddleware.Message{Body: body, RoutingKey: rk})
}

func SendAbort(mw newmiddleware.Middleware, rk string, clientID int) error {
	slog.Info("SEND ABORT", "clientID", clientID)
	body := batch.WriteAbort(clientID)
	return mw.Send(newmiddleware.Message{Body: body, RoutingKey: rk})
}
