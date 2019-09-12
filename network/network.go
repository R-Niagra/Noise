package network

import (
	"time"

	"github.com/guyu96/noise"
	"github.com/guyu96/noise/broadcast"
	"github.com/guyu96/noise/log"
	"github.com/guyu96/noise/protocol"
	"github.com/guyu96/noise/relay"
	kad "github.com/guyu96/noise/skademlia"
)

const (
	// DefaultBootstrapTimeout is the default timeout for bootstrapping with each peer.
	DefaultBootstrapTimeout = time.Second * 10
	// DefaultPeerThreshold is the default threshold above which bootstrapping is considered successful
	DefaultPeerThreshold = 8
)

// Network encapsulates the communication in a noise p2p network.
type Network struct {
	node          *noise.Node
	relayChan     chan relay.Message
	broadcastChan chan broadcast.Message
}

// New creates and returns a new network instance.
func New(host string, port uint16, keys *kad.Keypair) (*Network, error) {
	// Set up node and policy.
	params := noise.DefaultParams()
	params.Keys = keys
	params.Host = host
	params.Port = port
	node, err := noise.NewNode(params)
	if err != nil {
		return nil, err
	}

	r := relay.New()
	relayChan := r.GetRelayChan()
	bc := broadcast.New()
	broadcastChan := bc.GetBroadcastChan()

	policy := protocol.New()
	policy.Register(kad.New())
	policy.Register(r)
	policy.Register(bc)
	policy.Enforce(node)

	node.OnPeerInit(func(node *noise.Node, peer *noise.Peer) error {
		peer.OnConnError(func(node *noise.Node, peer *noise.Peer, err error) error {
			log.Info().Msgf("peer connection error: %v", err)
			return nil
		})
		return nil
	})

	go node.Listen()
	return &Network{
		node:          node,
		relayChan:     relayChan,
		broadcastChan: broadcastChan,
	}, nil
}

// Bootstrap bootstraps a network using a list of peer addresses and returns whether bootstrap finished before timeout.
func (ntw *Network) Bootstrap(peerAddrs []string, timeout time.Duration, peerThreshold int) bool {
	if len(peerAddrs) == 0 {
		return false
	}

	node := ntw.node
	nodeID := ntw.GetNodeID()
	nodeAddr := nodeID.Address()
	timer := time.NewTimer(timeout)
	doneChan := make(chan struct{})

	// Try each peer sequentially and use a goroutine for timeout control.
	go func() {
		for _, addr := range peerAddrs {
			if addr != nodeAddr {
				peer, err := node.Dial(addr)
				if err != nil {
					log.Warn().Msgf("%v: error dialing %v: %v", nodeAddr, addr, err)
				} else {
					kad.WaitUntilAuthenticated(peer)
					kad.FindNode(node, nodeID, kad.BucketSize(), 8) // 8 disjoint paths
					if ntw.GetNumPeers() >= DefaultPeerThreshold {
						doneChan <- struct{}{}
						break
					}
				}
			}
		}
		doneChan <- struct{}{}
	}()

	select {
	case <-doneChan:
		return true
	case <-timer.C:
		return false
	}
}

// BootstrapDefault runs Bootstrap with default parameters.
func (ntw *Network) BootstrapDefault(peerAddrs []string) bool {
	return ntw.Bootstrap(peerAddrs, DefaultBootstrapTimeout, DefaultPeerThreshold)
}

// GetRelayChan returns the channel for relay messages.
func (ntw *Network) GetRelayChan() chan relay.Message {
	return ntw.relayChan
}

// GetBroadcastChan returns the channel for broadcast messages.
func (ntw *Network) GetBroadcastChan() chan broadcast.Message {
	return ntw.broadcastChan
}

// GetNodeID returns the network node's skademlia ID.
func (ntw *Network) GetNodeID() kad.ID {
	return protocol.NodeID(ntw.node).(kad.ID)
}

// GetPeerAddrs returns the peer addresses in the network node's kademlia table.
func (ntw *Network) GetPeerAddrs() []string {
	return kad.Table(ntw.node).GetPeers()
}

//GetPeerKadID returns the KadID of the peer since it is randomly initialized sometimes
func (ntw *Network) GetPeerKadID(address string) kad.ID {
	return kad.Table(ntw.node).GetPeerByAddress(address)
}

//AddressFromPK returns the address associated with the public key
func (ntw *Network) AddressFromPK(publicKey []byte) string {
	return kad.Table(ntw.node).AddressFromPK(publicKey)
}

// GetNumPeers returns the number of peers the network node has.
func (ntw *Network) GetNumPeers() int {
	return len(ntw.GetPeerAddrs())
}

// Relay relays data to peer with given ID.
func (ntw *Network) Relay(peerID kad.ID, code byte, data []byte) error {
	nodeID := ntw.GetNodeID()
	nodeAddr := nodeID.Address()
	peerAddr := peerID.Address()
	msg := relay.NewMessage(nodeID, peerID, code, data)
	err := relay.ToPeer(ntw.node, msg, false, true)
	if err != nil {
		log.Warn().Msgf("%v to %v relay failed without lookup: %v", nodeAddr, peerAddr, err)
		err = relay.ToPeer(ntw.node, msg, true, true)
	}
	return err
}

// Broadcast broadcasts data to the entire p2p network.
func (ntw *Network) Broadcast(code byte, data []byte) {
	minBucketID := 0
	maxBucketID := kad.Table(ntw.node).GetNumOfBuckets() - 1
	broadcast.Send(ntw.node, ntw.GetNodeID(), code, data, minBucketID, maxBucketID, 0, true)
}
