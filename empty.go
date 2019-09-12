package noise

import "github.com/guyu96/noise/payload"

var _ Message = (*EmptyMessage)(nil)

type EmptyMessage struct{}

func (EmptyMessage) Read(reader payload.Reader) (Message, error) {
	return EmptyMessage{}, nil
}

func (EmptyMessage) Write() []byte {
	return nil
}
