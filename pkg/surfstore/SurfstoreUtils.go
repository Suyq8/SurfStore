package surfstore

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	//"golang.org/x/exp/slices"
)

var (
	emptyHashList = []string{EMPTYFILE_HASHVALUE}
	tombHashList  = []string{TOMBSTONE_HASHVALUE}
)

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func contain(a []string, b string) bool {
	for i := range a {
		if a[i] == b {
			return true
		}
	}
	return false
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Panicln(err)
	}

	// if index.db doesn't exist, it will return an empty map
	clientInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Panicln(err)
	}

	fileNames := make(map[string]struct{}, len(files))

	// update local metadata（update file)
	for _, file := range files {
		if valid := checkFileNameValid(file.Name()); !valid || file.IsDir() || file.Name() == DEFAULT_META_FILENAME {
			if !valid || file.IsDir() {
				log.Println("Invalid file name: ", file.Name())
			}
			continue
		}

		f, err := os.Open(filepath.Join(client.BaseDir, file.Name()))
		if err != nil {
			log.Panicln(err)
		}

		fileNames[file.Name()] = struct{}{}
		reader := bufio.NewReader(f)
		numBlock := int((file.Size() + client.BlockSize - 1) / client.BlockSize)
		fileHashList := make([]string, numBlock)
		buf := make([]byte, client.BlockSize)

		for idx := 0; idx < numBlock; idx++ {
			n, _ := io.ReadFull(reader, buf)
			fileHashList[idx] = GetBlockHashString(buf[:n])
		}

		if numBlock == 0 {
			fileHashList = emptyHashList
		}

		if fileMeta, ok := clientInfoMap[file.Name()]; ok {
			// file is modified
			if !equal(fileMeta.GetBlockHashList(), fileHashList) {
				clientInfoMap[file.Name()].BlockHashList = fileHashList
				clientInfoMap[file.Name()].Version++
			}
		} else {
			// new file
			clientInfoMap[file.Name()] = &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: fileHashList}
		}
	}

	// update local metadata（deleted file)
	for fileName, fileMeta := range clientInfoMap {
		if _, ok := fileNames[fileName]; !ok {
			// if not already deleted file
			if !equal(fileMeta.BlockHashList, tombHashList) {
				fileMeta.BlockHashList = tombHashList
				fileMeta.Version++
			}
		}
	}

	serverInfoMap := make(map[string]*FileMetaData)
	if err := client.GetFileInfoMap(&serverInfoMap); err != nil {
		log.Panicln(err)
	}

	// in client folder but not in server -> upload
	for fileName, clientMeta := range clientInfoMap {
		if serverMeta, ok := serverInfoMap[fileName]; ok {
			if serverMeta.Version >= clientMeta.Version {
				continue
			}
		}

		blockStoreMap := map[string][]string{}
		client.GetBlockStoreMap(clientMeta.BlockHashList, &blockStoreMap)

		if err := uploadFile(client, clientMeta, &blockStoreMap, clientInfoMap); err != nil {
			log.Panicln(err)
		}
	}

	// download file in server but not in client or file in legacy version
	for fileName, serverMeta := range serverInfoMap {
		if clientMeta, ok := clientInfoMap[fileName]; ok {
			if serverMeta.Version < clientMeta.Version || (serverMeta.Version == clientMeta.Version && equal(serverMeta.BlockHashList, clientMeta.BlockHashList)) {
				continue
			}
		}

		if err := downloadFile(client, fileName, clientInfoMap); err != nil {
			log.Panicln(err)
		}
	}

	if err := WriteMetaFile(clientInfoMap, client.BaseDir); err != nil {
		log.Panicln(err)
	}
}

func checkFileNameValid(fileName string) bool {
	return !(strings.Contains(fileName, ",") || strings.Contains(fileName, "/"))
}

func uploadFile(client RPCClient, clientMeta *FileMetaData, blockStoreMap *map[string][]string, clientInfoMap map[string]*FileMetaData) error {
	// put block if file is not empty or not deleted
	if !(equal(clientMeta.BlockHashList, emptyHashList) || equal(clientMeta.BlockHashList, tombHashList)) {
		hashInBlockStores := map[string][]string{}
		hashToAddr := make(map[string]string, len(clientMeta.BlockHashList))

		for addr, hashes := range *blockStoreMap {
			var hashInBlockStore []string
			if err := client.HasBlocks(hashes, addr, &hashInBlockStore); err != nil {
				log.Println("get block hash failed")
				return err
			}
			hashInBlockStores[addr] = hashInBlockStore

			for _, hash := range hashes {
				hashToAddr[hash] = addr
			}
		}

		f, err := os.Open(filepath.Join(client.BaseDir, clientMeta.Filename))
		if err != nil {
			log.Println("open file failed while upload file")
			return err
		}
		defer f.Close()

		reader := bufio.NewReader(f)
		numBlock := len(clientMeta.BlockHashList)
		buf := make([]byte, client.BlockSize)
		var succ bool

		for idx := 0; idx < numBlock; idx++ {
			n, _ := io.ReadFull(reader, buf)

			// if the block is not changed, skip it
			hash := clientMeta.BlockHashList[idx]
			addr := hashToAddr[hash]
			if !contain(hashInBlockStores[addr], hash) {
				block := &Block{BlockData: buf[:n], BlockSize: int32(n)}

				if err := client.PutBlock(block, addr, &succ); !(err == nil && succ) {
					log.Println("put block error", err, succ)
					return err
				}
			}
		}
	}

	// upload meta
	var version int32
	if err := client.UpdateFile(clientMeta, &version); err != nil {
		log.Println("update meta failed")
		return err
	}

	// when version is -1, download server file and update client metadata
	if version == -1 {
		log.Println("conflict version, download file in server")

		if err := downloadFile(client, clientMeta.Filename, clientInfoMap); err != nil {
			return err
		}
	}

	return nil
}

func downloadFile(client RPCClient, fileName string, clientInfoMap map[string]*FileMetaData) error {
	// get updated server metadata
	var serverInfoMap map[string]*FileMetaData
	if err := client.GetFileInfoMap(&serverInfoMap); err != nil {
		log.Println("get updated server metadata failed")
		return err
	}

	path := filepath.Join(client.BaseDir, fileName)
	clientInfoMap[fileName] = serverInfoMap[fileName]

	// if file is deleted
	if equal(serverInfoMap[fileName].BlockHashList, tombHashList) {
		// the file needed to be deleted may not exist?
		if err := os.Remove(path); err != nil {
			log.Println("remove file failed")
			// return err
		}
		return nil
	}

	f, err := os.Create(path)
	if err != nil {
		log.Println("create file failed")
		return err
	}
	defer f.Close()

	// if it's not an empty file
	if !equal(serverInfoMap[fileName].BlockHashList, emptyHashList) {
		hashToAddr := make(map[string]string, len(serverInfoMap[fileName].BlockHashList))
		blockStoreMap := map[string][]string{}
		client.GetBlockStoreMap(serverInfoMap[fileName].BlockHashList, &blockStoreMap)

		for addr, hashes := range blockStoreMap {
			for _, hash := range hashes {
				hashToAddr[hash] = addr
			}
		}

		var data []byte
		for _, hash := range serverInfoMap[fileName].BlockHashList {
			block := &Block{}
			if err := client.GetBlock(hash, hashToAddr[hash], block); err != nil {
				log.Println("get block failed")
				return err
			}

			data = append(data, block.BlockData...)
		}

		if _, err := f.Write(data); err != nil {
			log.Println("write file failed")
			return err
		}
	}

	return nil
}
