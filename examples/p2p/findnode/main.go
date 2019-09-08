package main

import (
	"bufio"
	"flag"
	"math/rand"
	"os"
	"time"

	"github.com/guyu96/noise"
	"github.com/guyu96/noise/log"
	"github.com/guyu96/noise/network"
	"github.com/guyu96/noise/protocol"
	kad "github.com/guyu96/noise/skademlia"
)

var (
	ip         = "127.0.0.1"
	bsAddr     = []string{"127.0.0.1:7000"}
	node       *noise.Node
	numPeers   = 30
	numBsPeers = 16
	peerFile   = "peers.txt"
)

func randID(ids []kad.ID) kad.ID {
	n := rand.Int() % len(ids)
	return ids[n]
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func randBsAddrs(ids []kad.ID) []string {
	n := minInt(len(ids), numBsPeers)
	bsAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		bsAddrs[i] = randID(ids).Address()
	}
	return bsAddrs
}

func main() {
	portFlag := flag.Uint("p", 7000, "")
	flag.Parse()
	rand.Seed(time.Now().Unix())
	reader := bufio.NewReader(os.Stdin)

	if *portFlag == 4000 {
		peers := kad.LoadIDs(peerFile)
		node, _, _ = network.InitNetwork(ip, *portFlag, randBsAddrs(peers), false, false)
		for {
			input, err := reader.ReadString('\n')
			if err != nil {
				panic(err)
			}
			switch input {
			case "fn\n":
				target := randID(peers)
				found := kad.FindNode(node, target, kad.BucketSize(), 8)
				log.Info().Msgf("closest peers to target %s: %+v", target.Address(), kad.IDAddresses(found))
			}
		}
	} else {
		peers := []kad.ID{}
		for i := 0; i < numPeers; i++ {
			node, _, _ = network.InitNetwork(ip, *portFlag, randBsAddrs(peers), false, false)
			*portFlag++
			peers = append(peers, protocol.NodeID(node).(kad.ID))
		}
		kad.PersistIDs(peerFile, peers)
		select {}
	}
}
