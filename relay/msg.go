package relay

import (
	"bytes"

	"github.com/guyu96/noise"
	"github.com/guyu96/noise/payload"
	kad "github.com/guyu96/noise/skademlia"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
)

const (
	hashSize = blake2b.Size256
)

// Message is a message with typed payload (Code and Data) to be relayed from From to To.
type Message struct {
	From      kad.ID
	To        kad.ID
	Hash      [hashSize]byte // TODO: Hash should be a method, not a field, since nodes should independently compute the hashes to be safe.
	SeenPeers []byte
	// Code is a single byte that indicates the type of Data so that Data can be properly deserialized.
	Code byte
	Data []byte
	// SeqNum is an incrementing sequence number (which wraps around 255, since it is of type byte). Messages with identical Code and Data but different SeqNums will hash differently.
	SeqNum byte
}

func (msg Message) Write() []byte {
	writer := payload.NewWriter(nil)
	writer.Write(msg.From.Write())
	writer.Write(msg.To.Write())
	writer.Write(msg.Hash[:])
	// Need to specify number of bytes for reader.ReadBytes method to work
	writer.WriteUint32(uint32(len(msg.SeenPeers)))
	writer.Write(msg.SeenPeers)
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

	To, err := kad.ID{}.Read(reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode To ID")
	}
	msg.To = To.(kad.ID)

	var hash [hashSize]byte
	for i := 0; i < hashSize; i++ {
		h, err := reader.ReadByte()
		if err != nil {
			return nil, err
		}
		hash[i] = h
	}
	msg.Hash = hash

	seenPeers, err := reader.ReadBytes()
	if err != nil {
		return nil, err
	}
	msg.SeenPeers = seenPeers

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

func (msg *Message) isSeenByPeer(peerID kad.ID) bool {
	if msg.From.Equals(peerID) {
		return true
	}
	hashSize := hashSize
	idHash := peerID.Hash()
	numSeenPeers := len(msg.SeenPeers) / hashSize
	for i := 0; i < numSeenPeers; i++ {
		peerIDHash := msg.SeenPeers[i*hashSize : (i+1)*hashSize]
		if bytes.Equal(idHash, peerIDHash) {
			return true
		}
	}
	return false
}

func (msg *Message) addSeenPeer(peerID kad.ID) {
	msg.SeenPeers = append(msg.SeenPeers, peerID.Hash()...)
}

// generateHash hashes everything but seen peers.
func (msg *Message) generateHash() {
	bytes := append(msg.From.Hash(), msg.To.Hash()...)
	bytes = append(bytes, msg.Code)
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
func NewMessage(from kad.ID, to kad.ID, code byte, data []byte) Message {
	msg := Message{
		From: from,
		To:   to,
		Code: code,
		Data: data,
	}
	msg.generateHash()
	return msg
}
