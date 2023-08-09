package surfstore

import (
	context "context"
	"log"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	metaStore     *MetaStore
	addr          string

	commitIndex    int64
	lastApplied    int64
	nextIndex      []int64
	pendingCommits []*chan bool

	serverId int64
	//rpcClients []RaftSurfstoreClient
	serverAddrs []string

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}

		if success.Flag {
			return s.metaStore.GetFileInfoMap(ctx, empty)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}

		if success.Flag {
			return s.metaStore.GetBlockStoreMap(ctx, &BlockHashes{Hashes: hashes.Hashes})
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	for {
		success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}

		if success.Flag {
			return s.metaStore.GetBlockStoreAddrs(ctx, empty)
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		s.isCrashedMutex.RLock()
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	s.isLeaderMutex.RLock()
	if !s.isLeader {
		s.isLeaderMutex.RUnlock()
		return nil, ERR_NOT_LEADER
	}
	s.isLeaderMutex.RUnlock()

	updateOPeration := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}

	s.log = append(s.log, updateOPeration)

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	go s.sendToAllFollowersInParallel(ctx)

	commit := <-commitChan

	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	for {
		s.isCrashedMutex.RLock()
		if s.isCrashed {
			s.isCrashedMutex.RUnlock()
			break
		}
		s.isCrashedMutex.RUnlock()

		resp := make(chan bool, len(s.serverAddrs)-1)
		for id, addr := range s.serverAddrs {
			if int64(id) != s.serverId {
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					log.Println("failed to connect", err)
				}

				client := NewRaftSurfstoreClient(conn)
				go s.sendToFollower(ctx, client, id, resp)
			}
		}

		totalResponses := 1
		totalAppends := 1

		// wait in loop for responses
		for {
			result := <-resp
			totalResponses++
			if result {
				totalAppends++
			}
			if totalResponses == len(s.serverAddrs) {
				break
			}
		}

		if totalAppends > len(s.serverAddrs)/2 {
			*s.pendingCommits[len(s.pendingCommits)-1] <- true
			s.commitIndex++
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, client RaftSurfstoreClient, serverId int, resp chan bool) {
	prevLogIndex, prevLogTerm := s.getLastLogIndexAndTerm(serverId)
	entries := s.log[prevLogIndex+1:]

	appendEntryInput := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}

	output, err := client.AppendEntries(ctx, appendEntryInput)

	if err != nil {
		resp <- false
	} else if output.Success {
		s.nextIndex[serverId] = output.MatchedIndex + 1
		resp <- true
	} else {
		s.nextIndex[serverId]--
		resp <- false
	}
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	appendEntryOutput := &AppendEntryOutput{
		Success:      false,
		ServerId:     s.serverId,
		Term:         s.term,
		MatchedIndex: -1,
	}

	// Reply false if term < currentTerm (stale request term)
	if input.Term < s.term {
		return appendEntryOutput, nil
	}

	// the term is out of date
	if input.Term > s.term {
		s.isLeaderMutex.Lock()
		s.isLeader = false
		s.term = input.Term
		appendEntryOutput.Term = s.term
		s.isLeaderMutex.Unlock()
	}

	if input.PrevLogIndex >= 0 {
		lastIdx := len(s.log) - 1

		// Reply false if log doesn’t contain an entry at prevLogIndex
		// whose term matches prevLogTerm
		if input.PrevLogIndex > int64(lastIdx) {
			return appendEntryOutput, nil
		}

		// If an existing entry conflicts with a new one (same index but
		// different terms), delete the existing entry and all that follow it
		if input.PrevLogTerm != s.log[input.PrevLogIndex].Term {
			return appendEntryOutput, nil
		}
	}

	s.log = s.log[:input.PrevLogIndex+1]

	// Append any new entries not already in the log
	s.log = append(s.log, input.Entries...)

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
	// index of last new entry)
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
	}

	for s.lastApplied < s.commitIndex {
		s.lastApplied++
		entry := s.log[s.lastApplied]
		s.metaStore.UpdateFile(ctx, entry.FileMetaData)
	}

	appendEntryOutput.Success = true
	appendEntryOutput.MatchedIndex = int64(len(s.log) - 1)

	return appendEntryOutput, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	s.term++
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	// initialize all nextIndex values to the index just after the last one in its log
	for i := 0; i < len(s.nextIndex); i++ {
		s.nextIndex[i] = int64(len(s.log))
	}

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	defer s.isCrashedMutex.RUnlock()

	if s.isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.RLock()
	defer s.isLeaderMutex.RUnlock()
	if !s.isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	cntChan := make(chan bool, len(s.serverAddrs))
	for id, addr := range s.serverAddrs {
		if int64(id) != s.serverId {
			prevLogIndex, prevLogTerm := s.getLastLogIndexAndTerm(id)

			input := &AppendEntryInput{
				Term:         s.term,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      s.log[s.nextIndex[id]:],
				LeaderCommit: s.commitIndex,
			}

			go func(addr string, cntChan chan bool, serverId int) {
				conn, err := grpc.Dial(addr, grpc.WithInsecure())
				if err != nil {
					log.Println("failed to connect", err)
				}

				client := NewRaftSurfstoreClient(conn)
				output, err := client.AppendEntries(ctx, input)

				if err != nil {
					cntChan <- false //cntChan <- 0
				} else if output.Success {
					s.nextIndex[serverId] = output.MatchedIndex + 1
					cntChan <- true
				} else {
					s.nextIndex[serverId]--
					cntChan <- true
				}
			}(addr, cntChan, id)
		}
	}

	totalResponses := 1
	totalAlive := 1

	// wait in loop for responses
	for {
		result := <-cntChan
		totalResponses++
		if result {
			totalAlive++
		}
		if totalResponses == len(s.serverAddrs) {
			break
		}
	}

	if totalAlive > len(s.serverAddrs)/2 {
		return &Success{Flag: true}, nil
	}

	return &Success{Flag: false}, nil
}

func (s *RaftSurfstore) getLastLogIndexAndTerm(id int) (int64, int64) {
	prevLogIndex := min(s.nextIndex[id], int64(len(s.log))) - 1
	var prevLogTerm int64
	if prevLogIndex < 0 {
		prevLogTerm = 0
	} else {
		prevLogTerm = s.log[prevLogIndex].Term
	}
	return prevLogIndex, prevLogTerm
}

func min(a, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
