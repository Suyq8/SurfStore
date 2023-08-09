package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
	fileName TEXT,
	version INT,
	hashIndex INT,
	hashValue TEXT
);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Println("Error During Meta Write Back")
			return err
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Println("Error During Meta Write Back")
		return err
	}
	defer db.Close()
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Println("Error During Meta Write Back")
		return err
	}
	statement.Exec()
	statement.Close()

	statement, err = db.Prepare(insertTuple)
	if err != nil {
		log.Println("Error During Meta Write Back")
		return err
	}
	defer statement.Close()

	for fileName, metaData := range fileMetas {
		version := metaData.Version
		for idx, hash := range metaData.BlockHashList {
			if _, err := statement.Exec(fileName, version, idx, hash); err != nil {
				log.Println("Failed writing into database")
				return err
			}
		}
	}

	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName, version FROM indexes;`

const getTuplesByFileName string = `SELECT hashValue FROM indexes WHERE fileName=?;`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}

	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Println("Error When Opening Meta")
		return fileMetaMap, err
	}
	defer db.Close()

	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Println(err)
		return fileMetaMap, err
	}
	defer rows.Close()

	var fileName string
	var version int32
	var hashValue string
	for rows.Next() {
		if err = rows.Scan(&fileName, &version); err != nil {
			log.Println(err)
			return fileMetaMap, err
		}
		fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: version}

		tuples, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Println(err)
			return fileMetaMap, err
		}
		defer tuples.Close()

		var blockHashList []string
		for tuples.Next() {
			if err = tuples.Scan(&hashValue); err != nil {
				log.Println(err)
				return fileMetaMap, err
			}
			blockHashList = append(blockHashList, hashValue)
		}
		fileMetaMap[fileName].BlockHashList = blockHashList
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
