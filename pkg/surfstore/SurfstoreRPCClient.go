package surfstore

import (
	context "context"
	"errors"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int64
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		return err
	}
	*succ = success.Flag

	return nil
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		return err
	}
	*blockHashesOut = blockHashes.Hashes

	return nil
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, outBlockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	defer conn.Close()
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	*outBlockHashes = blockHashes.Hashes

	return nil
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	for id, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			conn.Close()
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			fmt.Println(id, err)
			conn.Close()
			continue
		}
		*serverFileInfoMap = fileInfoMap.FileInfoMap
		return conn.Close()
	}

	return errors.New("no leader")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			conn.Close()
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		version, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			conn.Close()
			continue
		}
		*latestVersion = version.Version
		return conn.Close()
	}

	return errors.New("no leader")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, outBlockStoreMap *map[string][]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			conn.Close()
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockStoreMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			conn.Close()
			continue
		}
		for k, v := range blockStoreMap.BlockStoreMap {
			(*outBlockStoreMap)[k] = v.Hashes

		}
		return conn.Close()
	}

	return errors.New("no leader")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(outBlockStoreAddrs *[]string) error {
	for _, addr := range surfClient.MetaStoreAddrs {
		// connect to the server
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			conn.Close()
			continue
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		blockStoreAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			continue
		}
		*outBlockStoreAddrs = blockStoreAddrs.BlockStoreAddrs
		return conn.Close()
	}

	return errors.New("no leader")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      int64(blockSize),
	}
}
