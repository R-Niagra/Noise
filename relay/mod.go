package relay

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
	relayChanSize   = 64 // default relay channel buffer size
	numClosestPeers = 8  // find up to this many closest peers
	spreadFactor    = 1  // default relay spread factor
)

var (
	_           protocol.Block = (*block)(nil)
	relaySeqNum byte
)

// block stores necessary information for relaying.
type block struct {
	relayOpcode   noise.Opcode
	relayChan     chan Message
	relayMsgSeen  map[string]struct{} // TODO: make the map a LRU cache with limited capacity
	relayMsgMutex sync.Mutex
}

// New sets up and returns a new Relay block instance.
func New() *block {
	return &block{
		relayChan:    make(chan Message, relayChanSize),
		relayMsgSeen: map[string]struct{}{},
	}
}

func (b *block) GetRelayChan() chan Message {
	return b.relayChan
}

func (b *block) OnRegister(p *protocol.Protocol, node *noise.Node) {
	b.relayOpcode = noise.RegisterMessage(noise.NextAvailableOpcode(), (*Message)(nil))
}

func (b *block) OnBegin(p *protocol.Protocol, peer *noise.Peer, node *noise.Node) error {
	go b.handleRelayMessage(node, peer)
	return nil
}

func (b *block) OnEnd(p *protocol.Protocol, peer *noise.Peer) error {
	return nil
}

func (b *block) handleRelayMessage(node *noise.Node, peer *noise.Peer) {
	for {
		select {
		case msg := <-peer.Receive(b.relayOpcode):
			relayMsg := msg.(Message)
			msgHashStr := string(relayMsg.Hash[:])
			b.relayMsgMutex.Lock()
			if _, seen := b.relayMsgSeen[msgHashStr]; !seen {
				// Set seen flag to prevent relay loop, even though relay message also contains information on seen peers.
				b.relayMsgSeen[msgHashStr] = struct{}{}
				if relayMsg.To.Equals(protocol.NodeID(node)) {
					b.relayChan <- relayMsg
				} else {
					err := ToPeer(node, relayMsg, false, false)
					if err != nil {
						log.Warn().Msgf("%v relay failed without lookup: %v", node.InternalPort(), err)
						err = ToPeer(node, relayMsg, true, false)
					}
					if err != nil {
						log.Warn().Msgf("%v relay failed with lookup: %v", node.InternalPort(), err)
					}
				}
			}
			b.relayMsgMutex.Unlock()
		}
	}
}

// relayThroughPeer relays a message through peer with the given ID.
func relayThroughPeer(node *noise.Node, peerID kad.ID, msg Message, errChan chan error) {
	if peerID.Equals(protocol.NodeID(node)) {
		errChan <- fmt.Errorf("%v: cannot relay msg to ourselves", node.InternalPort())
		return
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

// ToPeer relays a message to peer synchronously.
func ToPeer(node *noise.Node, msg Message, doLookUp, incrementSeqNum bool) error {
	if msg.To.Equals(protocol.NodeID(node)) {
		return fmt.Errorf("cannot relay msg to ourselves")
	}
	// doLookup should be set to true when relay without lookup fails.
	if doLookUp {
		kad.FindNode(node, msg.To, 3, 4) // TODO: adjust the alpha and number of disjoint paths after doing simulation
	}
	// incrementSeqNum should only be set to true when ToPeer is called by the "from" node (i.e. not a relay node).
	if incrementSeqNum {
		msg.ChangeSeqNum(relaySeqNum)
		relaySeqNum++
	}
	closestIDs := kad.FindClosestPeers(kad.Table(node), msg.To.Hash(), numClosestPeers)
	errChan := make(chan error)

	// Determine if direct messaging is possible and filter out seen peers
	direct := false
	unseenIDs := []kad.ID{}
	for _, id := range closestIDs {
		kadID := id.(kad.ID)
		if kadID.Equals(msg.To) {
			direct = true
			break
		}
		if !msg.isSeenByPeer(kadID) {
			unseenIDs = append(unseenIDs, kadID)
		}
	}

	if direct {
		go relayThroughPeer(node, msg.To, msg, errChan)
		log.Info().Msgf("%v direct message to %v", node.InternalPort(), msg.To.Address())
		select {
		case err := <-errChan:
			return err
		}
	} else {
		if len(unseenIDs) == 0 { // no more peers to relay through
			return fmt.Errorf("ran out of peers to relay through")
		}
		if len(unseenIDs) > spreadFactor {
			unseenIDs = unseenIDs[:spreadFactor]
		}
		for _, id := range unseenIDs {
			msg.addSeenPeer(id)
		}
		for _, id := range unseenIDs {
			go relayThroughPeer(node, id, msg, errChan)
			log.Info().Msgf("%v indirect relay via %v", node.InternalPort(), id.Address())
		}

		numUnseenPeers := uint32(len(unseenIDs))
		errCount := uint32(0)
		// We only require one success and the relay fails only if all peers fail
		for atomic.LoadUint32(&errCount) < numUnseenPeers {
			select {
			case err := <-errChan:
				if err != nil {
					log.Warn().Err(err)
					atomic.AddUint32(&errCount, 1)
				} else {
					return nil
				}
			}
		}
		return fmt.Errorf("%v relay failed with all peers", node.InternalPort())
	}
}
