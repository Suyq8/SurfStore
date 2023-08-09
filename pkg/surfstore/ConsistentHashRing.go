package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
	Hashes    []string
}

func (c ConsistentHashRing) GetResponsibleServer(blockHash string) string {
	idx := sort.SearchStrings(c.Hashes, blockHash)
	if idx == len(c.Hashes) {
		return c.ServerMap[c.Hashes[0]]
	}
	return c.ServerMap[c.Hashes[idx]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentHashRing := ConsistentHashRing{ServerMap: map[string]string{}}
	hashes := make([]string, 0, len(serverAddrs))

	for _, serveraddr := range serverAddrs {
		serverName := fmt.Sprintf("blockstore%v", serveraddr)
		hash := consistentHashRing.Hash(serverName)
		consistentHashRing.ServerMap[hash] = serveraddr
		hashes = append(hashes, hash)
	}

	sort.Strings(hashes)
	consistentHashRing.Hashes = hashes
	return &consistentHashRing
}
