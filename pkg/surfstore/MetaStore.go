package surfstore

import (
	context "context"
	sync "sync"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	mtx                sync.Mutex
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	newVersion := fileMetaData.GetVersion()
	fileName := fileMetaData.GetFilename()
	m.mtx.Lock()

	if metaData, ok := m.FileMetaMap[fileName]; ok {
		oldVersion := metaData.GetVersion()

		// invalid version, including tombstone
		if newVersion-oldVersion != 1 {
			m.mtx.Unlock()
			return &Version{Version: -1}, nil
		}
	} else if fileMetaData.GetVersion() != 1 { // create file with a wrong version
		m.mtx.Unlock()
		return &Version{Version: -1}, nil
	}

	m.FileMetaMap[fileName] = fileMetaData
	m.mtx.Unlock()
	return &Version{Version: newVersion}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	n := len(m.BlockStoreAddrs)
	blockStoreMap := make(map[string]*BlockHashes, n)

	for _, hash := range blockHashesIn.Hashes {
		blockServer := m.ConsistentHashRing.GetResponsibleServer(hash)
		if blockHashes, ok := blockStoreMap[blockServer]; ok {
			blockHashes.Hashes = append(blockHashes.Hashes, hash)
		} else {
			blockStoreMap[blockServer] = &BlockHashes{Hashes: []string{hash}}
		}
	}

	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
