package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}
	//clients := make([]RaftSurfstoreClient, len(config.RaftAddrs))

	//for idx, addr := range config.RaftAddrs {
	//	if int64(idx) != id {
	//		conn, err := grpc.Dial(addr, grpc.WithInsecure())
	//		if err != nil {
	//			log.Println("failed to connect", err)
	//		}
	//
	//		client := NewRaftSurfstoreClient(conn)
	//		clients[idx] = client
	//	}
	//}

	server := RaftSurfstore{
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		log:            make([]*UpdateOperation, 0),
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		serverId:       id,
		commitIndex:    -1,
		lastApplied:    -1,
		nextIndex:      make([]int64, len(config.RaftAddrs)),
		pendingCommits: make([]*chan bool, 0),
		//rpcClients:     clients,
		addr:        config.RaftAddrs[id],
		serverAddrs: config.RaftAddrs,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)

	listener, err := net.Listen("tcp", server.addr)
	if err != nil {
		log.Panicln(err)
		return err
	}

	return s.Serve(listener)
}
