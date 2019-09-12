package main

import (
	"bufio"
	"flag"
	"os"
	"sync/atomic"

	"github.com/guyu96/noise/log"
	"github.com/guyu96/noise/network"
	kad "github.com/guyu96/noise/skademlia"
)

var (
	host              = "127.0.0.1"
	minPort           = uint16(9000)
	activePort        = uint16(8000)
	numPeers          = 32
	numBootstrapPeers = 4
	peersFile         = "peers.txt"
	keypairsFile      = "keypairs.txt"
)

func main() {
	createFlag := flag.Bool("create-peers", false, "")
	portFlag := flag.Uint("p", uint(minPort), "")
	flag.Parse()

	if *createFlag {
		peers, keypairs, err := network.CreatePeers(host, minPort, numPeers)
		if err != nil {
			panic(err)
		}
		kad.PersistIDs(peersFile, peers)
		kad.PersistKeypairs(keypairsFile, keypairs)
		log.Info().Msgf("created %v peers", numPeers)
		return
	}

	peers := kad.LoadIDs(peersFile)
	keypairs := kad.LoadKeypairs(keypairsFile)
	if uint16(*portFlag) == activePort {
		peerAddrs := network.RandBootstrapAddrs(peers, numBootstrapPeers)
		ntw, err := network.New(host, activePort, kad.RandomKeys())
		if err != nil {
			panic(err)
		}
		doneBeforeTimeout := ntw.BootstrapDefault(peerAddrs)
		log.Info().Msgf("connected to %v peers: | done before timeout: %v", ntw.GetNumPeers(), doneBeforeTimeout)

		reader := bufio.NewReader(os.Stdin)
		for {
			input, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}
			ntw.Broadcast(0, []byte(input))
		}
	} else {
		rcvCount := uint32(0)
		_, broadcastChan := network.RunPeers(peers, keypairs, numBootstrapPeers)
		for {
			select {
			case msg := <-broadcastChan:
				atomic.AddUint32(&rcvCount, 1)
				log.Info().Msgf("msg: %s, total rcv count: %d", msg.Data, atomic.LoadUint32(&rcvCount))
			}
		}
	}
}
