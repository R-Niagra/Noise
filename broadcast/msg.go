package broadcast

import (
	"github.com/guyu96/noise"
	"github.com/guyu96/noise/payload"
	kad "github.com/guyu96/noise/skademlia"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
)

const (
	hashSize = blake2b.Size256
)

// Message is a message with typed payload (Code and Data) to be broadcasted. Unlike relay messages, broadcast messages do not have SeenPeers because the number of seen peers could be much larger.
type Message struct {
	From      kad.ID
	PrefixLen uint16
	Hash      [hashSize]byte
	// Code is a single byte that indicates the type of Data so that Data can be properly deserialized.
	Code byte
	Data []byte
	// SeqNum is an incrementing sequence number (which wraps around 255, since it is of type byte). Messages with identical Code and Data but different SeqNums will hash differently.
	SeqNum byte
}

func (msg Message) Write() []byte {
	writer := payload.NewWriter(nil)
	writer.Write(msg.From.Write())
	writer.WriteUint16(msg.PrefixLen)
	// Need to specify number of bytes for reader.ReadBytes method to work
	writer.Write(msg.Hash[:])
	writer.WriteByte(msg.Code)
	writer.WriteUint32(uint32(len(msg.Data)))
	writer.Write(msg.Data)
	writer.WriteByte(msg.SeqNum)
	return writer.Bytes()
}

func (msg Message) Read(reader payload.Reader) (noise.Message, error) {
	From, err := kad.ID{}.Read(reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode From ID")
	}
	msg.From = From.(kad.ID)

	prefixlen, err := reader.ReadUint16()
	if err != nil {
		return nil, err
	}
	msg.PrefixLen = prefixlen

	var hash [hashSize]byte
	for i := 0; i < hashSize; i++ {
		h, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		hash[i] = h
	}
	msg.Hash = hash

	code, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	msg.Code = code

	data, err := reader.ReadBytes()
	if err != nil {
		return nil, err
	}
	msg.Data = data

	seqNum, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	msg.SeqNum = seqNum

	return msg, nil
}

// generateHash hashes everything but prefixLen because we want the hash to be the same even when the message is broadcast by different nodes (which might result in different prefixLens).
func (msg *Message) generateHash() {
	bytes := append(msg.From.Hash(), msg.Code)
	bytes = append(bytes, msg.Data...)
	bytes = append(bytes, msg.SeqNum)
	msg.Hash = blake2b.Sum256(bytes)
}

// ChangeSeqNum changes the message sequence number and rehashes the message.
func (msg *Message) ChangeSeqNum(newSeqNum byte) {
	msg.SeqNum = newSeqNum
	msg.generateHash()
}

// NewMessage creates a new Message instance.
func NewMessage(from kad.ID, prefixlen uint16, code byte, data []byte) Message {
	msg := Message{
		From:      from,
		PrefixLen: prefixlen,
		Code:      code,
		Data:      data,
	}
	msg.generateHash()
	return msg
}
