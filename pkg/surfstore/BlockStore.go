package surfstore

import (
	context "context"
	"errors"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	mtx      sync.Mutex
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	bs.mtx.Lock()
	if b, ok := bs.BlockMap[blockHash.Hash]; ok {
		bs.mtx.Unlock()
		return b, nil
	} else {
		bs.mtx.Unlock()
		return nil, errors.New("block not found")
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	hash := GetBlockHashString(block.GetBlockData())
	bs.mtx.Lock()
	if _, ok := bs.BlockMap[hash]; ok {
		bs.mtx.Unlock()
		return &Success{Flag: true}, errors.New("hash existed")
	}
	bs.BlockMap[hash] = block
	bs.mtx.Unlock()

	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	var hashes []string
	bs.mtx.Lock()
	for _, hash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[hash]; ok {
			hashes = append(hashes, hash)
		}
	}
	bs.mtx.Unlock()

	return &BlockHashes{Hashes: hashes}, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	bs.mtx.Lock()
	blockMap := bs.BlockMap
	hashes := make([]string, 0, len(blockMap))
	for k := range blockMap {
		hashes = append(hashes, k)
	}
	bs.mtx.Unlock()

	return &BlockHashes{Hashes: hashes}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
