package envelope

import "tp-grupal-distribuidos/internal/common/messageprotocol/wire"

type Header struct {
	ClientID int
	QueryID  uint8
	Type     uint8
}

func (h Header) WriteTo(w *wire.Writer) {
	w.Uint32(uint32(h.ClientID))
	w.Uint8(h.QueryID)
	w.Uint8(h.Type)
}

func ReadFrom(r *wire.Reader) Header {
	return Header{
		ClientID: int(r.Uint32()),
		QueryID:  r.Uint8(),
		Type:     r.Uint8(),
	}
}
