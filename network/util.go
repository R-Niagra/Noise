package network

import (
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/guyu96/noise/broadcast"
	"github.com/guyu96/noise/log"
	"github.com/guyu96/noise/relay"
	kad "github.com/guyu96/noise/skademlia"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

// CreatePeers creates a number of peer identities starting at the minimum port number.
func CreatePeers(host string, minPort uint16, numPeers int) ([]kad.ID, []*kad.Keypair, error) {
	peers := []kad.ID{}
	keypairs := []*kad.Keypair{}
	for i := 0; i < numPeers; i++ {
		port := minPort + uint16(i)
		keys := kad.RandomKeys()
		ntw, err := New(host, port, keys)
		if err != nil {
			return nil, nil, err
		}
		peers = append(peers, ntw.GetNodeID())
		keypairs = append(keypairs, keys)
	}
	return peers, keypairs, nil
}

// RunPeers runs the specified peers, bootstrap them with each other, and returns two channels that aggregate all relay and broadcast messages.
func RunPeers(peers []kad.ID, keypairs []*kad.Keypair, numBootstrapPeers int) (chan relay.Message, chan broadcast.Message) {
	if len(peers) != len(keypairs) {
		panic("peers and keypairs have different lengths")
	}
	// First, spawn peer networks without bootstrapping.
	var wg sync.WaitGroup
	var mtx sync.Mutex
	peerNetworks := []*Network{}
	for i, id := range peers {
		wg.Add(1)
		go func(addr string, keys *kad.Keypair) {
			defer wg.Done()
			host, port, err := AddrToHostPort(addr)
			if err != nil {
				log.Warn().Err(err)
				return
			}
			ntw, err := New(host, port, keys)
			if err != nil {
				log.Warn().Err(err)
				return
			}
			mtx.Lock()
			peerNetworks = append(peerNetworks, ntw)
			mtx.Unlock()
		}(id.Address(), keypairs[i])
	}
	wg.Wait()
	log.Info().Msg("PEERS SPAWNED")

	// Next, bootstrap peer networks with random bootstrap addresses.
	rand.Seed(time.Now().Unix())
	for _, ntw := range peerNetworks {
		wg.Add(1)
		mtx.Lock()
		// Note that it is possible to get some dead peers in the bootstrap list.
		bootstrapAddrs := RandBootstrapAddrs(peers, numBootstrapPeers)
		mtx.Unlock()
		go func(ntw *Network) {
			defer wg.Done()
			doneBeforeTimeout := ntw.BootstrapDefault(bootstrapAddrs)
			log.Info().Msgf("connected to %v peers: | done before timeout: %v", ntw.GetNumPeers(), doneBeforeTimeout)
		}(ntw)
		// Space out the bootstrap calls to avoid local network errors.
		time.Sleep(time.Millisecond * 100)
	}
	wg.Wait()
	log.Info().Msg("BOOTSTRAP DONE")

	// Finally, redirect all networks' relay and broadcast messages to master channels.
	masterRelayChan := make(chan relay.Message)
	masterBroadcastChan := make(chan broadcast.Message)
	for _, ntw := range peerNetworks {
		go func(ntw *Network) {
			for {
				select {
				case msg := <-ntw.GetRelayChan():
					masterRelayChan <- msg
				case msg := <-ntw.GetBroadcastChan():
					masterBroadcastChan <- msg
				}
			}
		}(ntw)
	}

	return masterRelayChan, masterBroadcastChan
}

// AddrToHostPort splits an address-colon-host string into host and port.
func AddrToHostPort(addr string) (string, uint16, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("error extracting host and port from %v: %v", addr, err)
	}
	portUint, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("error converting port %v to uint16: %v", port, err)
	}
	return host, uint16(portUint), nil
}

// RandBootstrapAddrs returns up to n random addresses from peers.
func RandBootstrapAddrs(peers []kad.ID, n int) []string {
	numPeers := len(peers)
	addrs := make([]string, numPeers)
	for i := 0; i < numPeers; i++ {
		addrs[i] = peers[i].Address()
	}
	if n >= numPeers {
		return addrs
	}
	rand.Shuffle(numPeers, func(i, j int) {
		addrs[i], addrs[j] = addrs[j], addrs[i]
	})
	return addrs[:n]
}

// RandPeer returns a random peer from peers.
func RandPeer(peers []kad.ID) kad.ID {
	numPeers := len(peers)
	if numPeers == 0 {
		panic("peers is empty")
	}
	return peers[rand.Int()%numPeers]
}
