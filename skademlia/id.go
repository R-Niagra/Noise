package skademlia

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/bits"
	"os"
	"strings"

	"github.com/guyu96/noise"
	"github.com/guyu96/noise/log"
	"github.com/guyu96/noise/payload"
	"github.com/guyu96/noise/protocol"
	"github.com/pkg/errors"
	"golang.org/x/crypto/blake2b"
)

var (
	_ protocol.ID = (*ID)(nil)
)

// ID contains S/Kademlia node identity & address info.
type ID struct {
	address   string
	publicKey []byte

	hash  []byte // publicKey hash
	nonce []byte
}

func NewID(address string, publicKey, nonce []byte) ID {
	hash := blake2b.Sum256(publicKey)

	return ID{
		address:   address,
		publicKey: publicKey,

		hash:  hash[:],
		nonce: nonce,
	}
}

func (a ID) Read(reader payload.Reader) (msg noise.Message, err error) {
	a.address, err = reader.ReadString()
	if err != nil {
		return nil, errors.Wrap(err, "skademlia: failed to deserialize ID address")
	}

	a.publicKey, err = reader.ReadBytes()
	if err != nil {
		return nil, errors.Wrap(err, "skademlia: failed to deserialize ID public key")
	}

	hash := blake2b.Sum256(a.publicKey)
	a.hash = hash[:]

	a.nonce, err = reader.ReadBytes()
	if err != nil {
		return nil, errors.Wrap(err, "skademlia: failed to deserialize ID nonce")
	}

	return a, nil
}

func (a ID) Write() []byte {
	return payload.NewWriter(nil).
		WriteString(a.address).
		WriteBytes(a.publicKey).
		WriteBytes(a.nonce).
		Bytes()
}

func (a ID) PublicKey() []byte {
	return a.publicKey
}

func (a ID) Hash() []byte {
	return a.hash
}

func (a ID) Address() string {
	return a.address
}

func (a ID) Equals(other protocol.ID) bool {
	if other, ok := other.(ID); ok {
		return bytes.Equal(a.hash, other.hash)
	}

	return false
}

func (a ID) String() string {
	return fmt.Sprintf("S/Kademlia(address: %s, publicKey: %x, hash: %x, nonce: %x)", a.address, a.publicKey[:16], a.hash[:16], a.nonce)
}

func check(e error) {
	if e != nil {
		log.Fatal().Msgf("%s", e)
	}
}

func (a ID) toCSV() string {
	return fmt.Sprintf("%s,%x,%x,%x\n", a.address, a.publicKey, a.hash, a.nonce)
}

func (a ID) ToCsvBytes() []byte {
	return []byte(a.toCSV())
}

func FromString(s string) ID {
	fields := strings.Split(s, ",")
	id := &ID{}
	id.address = fields[0]
	id.publicKey = hexToBytes(fields[1])
	id.hash = hexToBytes(fields[2])
	id.nonce = hexToBytes(fields[3][:len(fields[3])-1])
	return *id
}

func PersistIDs(filepath string, ids []ID) {
	f, err := os.Create(filepath)
	check(err)
	defer f.Close()

	for _, id := range ids {
		_, err := f.WriteString(id.toCSV())
		check(err)
	}
}

func LoadIDs(filepath string) (ids []ID) {
	f, err := os.Open(filepath)
	check(err)
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		l, err := r.ReadString('\n')
		if err != nil {
			break
		}
		l = strings.TrimSpace(l)
		ids = append(ids, FromString(l))
	}
	return ids
}

func IDAddresses(ids []ID) (addrs []string) {
	for _, id := range ids {
		addrs = append(addrs, id.Address())
	}
	return addrs
}

func hexToBytes(s string) []byte {
	val, err := hex.DecodeString(s)
	check(err)
	return val
}

func prefixLen(hash []byte) int {
	for i, b := range hash {
		if b != 0 {
			return i*8 + bits.LeadingZeros8(uint8(b))
		}
	}

	return len(hash)*8 - 1
}

func xor(a, b []byte) []byte {
	if len(a) != len(b) {
		panic(fmt.Sprintf("skademlia xor: length mismatch %d, %d", len(a), len(b)))
	}

	c := make([]byte, len(a))

	for i := 0; i < len(a); i++ {
		c[i] = a[i] ^ b[i]
	}

	return c
}

func xorPriority(a, b []byte) uint64 {
	return binary.BigEndian.Uint64(xor(a, b))
}

func prefixDiff(a, b []byte, n int) int {
	hash, total := xor(a, b), 0

	for i, b := range hash {
		if n <= 8*i {
			break
		} else if n > 8*i && n < 8*(i+1) {
			shift := 8 - uint(n%8)
			b = b >> shift
		}
		total += bits.OnesCount8(uint8(b))
	}
	return total
}
