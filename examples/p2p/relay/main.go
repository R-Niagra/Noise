package main

import (
	"bufio"
	"flag"
	"os"

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
			peerID := network.RandPeer(peers)
			input, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}
			log.Info().Msgf("relaying msg to %v", peerID.Address())
			if err := ntw.Relay(peerID, 0, []byte(input)); err != nil {
				log.Warn().Msgf("relay failed with lookup: %v", err)
			}
		}
	} else {
		relayChan, _ := network.RunPeers(peers, keypairs, numBootstrapPeers)
		for {
			select {
			case msg := <-relayChan:
				log.Info().Msgf("rcv relay msg: %s", msg.Data)
			}
		}
	}
}
