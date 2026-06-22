package envelope

import "tp-grupal-distribuidos/internal/common/messageprotocol/wire"

const HeaderSize = wire.Uint32Size + 3*wire.Uint8Size + wire.Uint64Size

type Header struct {
	ClientID int
	QueryID  uint8
	Type     uint8
	SenderID uint8
	Seq      uint64
}

func (h Header) WriteTo(w *wire.Writer) {
	w.Uint32(uint32(h.ClientID))
	w.Uint8(h.QueryID)
	w.Uint8(h.Type)
	w.Uint8(h.SenderID)
	w.Uint64(h.Seq)
}

func ReadFrom(r *wire.Reader) Header {
	return Header{
		ClientID: int(r.Uint32()),
		QueryID:  r.Uint8(),
		Type:     r.Uint8(),
		SenderID: r.Uint8(),
		Seq:      r.Uint64(),
	}
}
