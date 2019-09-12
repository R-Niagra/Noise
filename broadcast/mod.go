package broadcast

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/guyu96/noise"
	"github.com/guyu96/noise/log"
	"github.com/guyu96/noise/protocol"
	kad "github.com/guyu96/noise/skademlia"
)

const (
	broadcastChanSize = 64 // default broadcast channel buffer size
)

var (
	_               protocol.Block = (*block)(nil)
	broadcastSeqNum byte
)

// block stores necessary information for a broadcasting.
type block struct {
	broadcastOpcode  noise.Opcode
	broadcastChan    chan Message
	broadcastMsgSeen map[string]struct{} // TODO: make the map a LRU cache with limited capacity
	broadcastMutex   sync.Mutex
}

// New sets up and returns a new Broadcast block instance.
func New() *block {
	return &block{
		broadcastChan:    make(chan Message, broadcastChanSize),
		broadcastMsgSeen: map[string]struct{}{},
	}
}

func (b *block) GetBroadcastChan() chan Message {
	return b.broadcastChan
}

func (b *block) OnRegister(p *protocol.Protocol, node *noise.Node) {
	b.broadcastOpcode = noise.RegisterMessage(noise.NextAvailableOpcode(), (*Message)(nil))
}

func (b *block) OnBegin(p *protocol.Protocol, peer *noise.Peer, node *noise.Node) error {
	go b.handleBroadcastMessage(node, peer)
	return nil
}

func (b *block) OnEnd(p *protocol.Protocol, peer *noise.Peer) error {
	return nil
}

func (b *block) handleBroadcastMessage(node *noise.Node, peer *noise.Peer) {
	for {
		select {
		case msg := <-peer.Receive(b.broadcastOpcode):
			broadcastMsg := msg.(Message)
			msgHashStr := string(broadcastMsg.Hash[:])
			b.broadcastMutex.Lock()
			if _, seen := b.broadcastMsgSeen[msgHashStr]; !seen {
				b.broadcastMsgSeen[msgHashStr] = struct{}{}
				b.broadcastChan <- broadcastMsg
				minBucketID := int(broadcastMsg.PrefixLen)           // minimum common prefix length
				maxBucketID := kad.Table(node).GetNumOfBuckets() - 1 // maximum common prefix length
				Send(node, broadcastMsg.From, broadcastMsg.Code, broadcastMsg.Data, minBucketID, maxBucketID, broadcastMsg.SeqNum, false)
			}
			b.broadcastMutex.Unlock()
		}
	}
}

// broadcastThroughPeer broadcasts a message through peer with the given ID.
func broadcastThroughPeer(node *noise.Node, peerID kad.ID, msg Message, errChan chan error) {
	if peerID.Equals(protocol.NodeID(node)) {
		errChan <- fmt.Errorf("%v: cannot broadcast msg to ourselves", node.InternalPort())
	}

	peer := protocol.Peer(node, peerID)

	if peer == nil {
		peer, err := node.Dial(peerID.Address())
		if err != nil {
			errChan <- fmt.Errorf("%v: cannot reach peer at %v", node.InternalPort(), peerID.Address())
			return
		}
		kad.WaitUntilAuthenticated(peer)
	}

	errChan <- peer.SendMessage(msg)
}

// Send starts broadcasting data to the network.
func Send(node *noise.Node, from kad.ID, code byte, data []byte, minBucketID int, maxBucketID int, seqNum byte, incrementSeqNum bool) {
	errChan := make(chan error)
	// TODO: maybe do a self node lookup here
	peers, prefixLens := kad.Table(node).GetBroadcastPeers(minBucketID, maxBucketID)
	for i, id := range peers {
		msg := NewMessage(from, prefixLens[i], code, data)
		// If incrementSeqNum is true, then seqNum is ignored and broadcastSeqNum is used and incremented instead. incrementSeqNum should only be set to true when Send is Send is called by the "from" node (i.e. not an intermediate broadcast node).
		if incrementSeqNum {
			msg.ChangeSeqNum(broadcastSeqNum)
		} else {
			msg.ChangeSeqNum(seqNum)
		}
		go broadcastThroughPeer(node, id.(kad.ID), msg, errChan)
	}
	if incrementSeqNum {
		broadcastSeqNum++
	}

	numPeers := uint32(len(peers))
	responseCount := uint32(0)
	for atomic.LoadUint32(&responseCount) < numPeers {
		select {
		case err := <-errChan:
			if err != nil {
				log.Warn().Err(err)
			}
			atomic.AddUint32(&responseCount, 1)
		}
	}
}
