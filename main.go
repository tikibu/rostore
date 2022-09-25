package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/tidwall/redcon"
	"github.com/tikibu/rostore/handler"
	"github.com/tikibu/rostore/store"
)

var addr = ":6380"

/*func mockStore() (*store.Store, error) {
	records := store.MockRecords()

	// let's build a mock data jsonl file
	recordsBytes := store.MockJsonlBytes(records)

	//ioutil.WriteFile("test_data/records.jsonl", recordsBytes, 0644)

	index, err := store.BuildIndex(bytes.NewReader(recordsBytes))
	if err != nil {
		return nil, err
	}
	indexBuf := bytes.Buffer{}
	err = index.WriteJsonl(&indexBuf)
	if err != nil {
		return nil, err
	}

	//ioutil.WriteFile("test_data/mock_index.jsonl", indexBuf.Bytes(), 0644)

	//let's build a store
	store, err := store.NewStoreFromRecordsWithIndex(func() (io.ReadSeekCloser, error) {
		return store.NewReadSeekCloser(bytes.NewReader(recordsBytes)), nil
	},
		bytes.NewReader(indexBuf.Bytes()))

	if err != nil {
		return nil, err
	}
	return store, nil
}*/

type StoreConfig struct {
	RecordsFileName string `json:"records_file_name"`
	IndexFileName   string `json:"index_file_name,omitempty"`
}

func loadStore(storeConfig StoreConfig) (*store.Store, error) {
	if storeConfig.RecordsFileName == "" {
		return nil, fmt.Errorf("records file name is empty")
	}

	buildIndex := true
	if storeConfig.IndexFileName == "" {
		buildIndex = false
	}
	recordsFileName := storeConfig.RecordsFileName
	var store_ *store.Store

	var indexFile io.ReadCloser
	if !buildIndex {
		var err error
		indexFile, err = os.Open(storeConfig.IndexFileName)
		if err != nil {
			buildIndex = true
		}

	}

	if buildIndex {
		var err error
		store_, err = store.NewStoreFromRecords(func() (io.ReadSeekCloser, error) {
			return os.Open(recordsFileName)
		})
		if err != nil {
			return nil, err
		}
		return store_, nil
	} else {
		//let's build a store
		var err error
		recordsFileName := storeConfig.RecordsFileName

		store_, err = store.NewStoreFromRecordsWithIndex(func() (io.ReadSeekCloser, error) {
			return os.Open(recordsFileName)
		}, indexFile)

		if err != nil {
			return nil, err
		}
	}
	return store_, nil
}

func readConfigFromFile(configFileName string) (storeConfig *StoreConfig, lastModifed *time.Time, err error) {
	stats, err := os.Stat(configFileName)
	if err != nil {
		return nil, nil, err
	}
	modTime := stats.ModTime()
	lastModifed = &modTime

	b, err := ioutil.ReadFile(configFileName)
	if err != nil {
		return nil, nil, err
	}

	storeConfig = &StoreConfig{}
	err = json.Unmarshal(b, &storeConfig)
	if err != nil {
		return nil, nil, err
	}

	return storeConfig, lastModifed, nil
}

func generateIndex(recordsFileName string, indexFileName string) error {
	recordsFile, err := os.Open(recordsFileName)
	if err != nil {
		return err
	}

	index, err := store.BuildIndex(recordsFile)
	if err != nil {
		return err
	}
	// open file for writing
	indexFile, err := os.OpenFile(indexFileName, os.O_WRONLY|os.O_CREATE, 0644)
	err = index.WriteJsonl(indexFile)
	if err != nil {
		return err
	}
	indexFile.Close()
	return nil
}

func main() {
	onlyGenerateIndex := flag.Bool("only_generate_index", false, "only generate index")
	recordsFileName := flag.String("records_file_name", "", "records file name for index generation")
	indexFileName := flag.String("index_file_name", "", "records file name for index generation")
	addr := flag.String("addr", "localhost:6380", "addr to listen on")

	configFileName := flag.String("config_file_name", "config.json", "config file name, with records file name and index file name")
	checkConfigInterval := flag.Duration("check-config-interval", 5*time.Second, "check config file interval")
	flag.Parse()

	//generate index and exit
	if *onlyGenerateIndex {
		err := generateIndex(*recordsFileName, *indexFileName)
		if err != nil {
			log.Fatal(err)
		}
		return
	}
	//lets read config from file
	storeConfig, lastModifed, err := readConfigFromFile(*configFileName)
	if err != nil {
		log.Fatal(err)
	}

	//let's load store
	store_, err := loadStore(*storeConfig)
	if err != nil {
		log.Fatal(err)
	}

	/*log.Printf("started server at %s", addr)
	store, err := mockStore()
	if err != nil {
		log.Fatal(err)
	}*/

	handler := handler.NewHandler(store_)
	//config reload loop
	go func() {
		for {
			time.Sleep(*checkConfigInterval)

			storeConfig, modified, err := readConfigFromFile(*configFileName)
			if err != nil {
				log.Println(fmt.Errorf("Failed to load a config %w", err))
				continue

			}
			if modified != nil && !modified.Equal(*lastModifed) {
				store_, err := loadStore(*storeConfig)
				if err != nil {
					log.Println(fmt.Errorf("Failed to load a store %w", err))
				} else {
					handler.Store = store_
				}

			}

		}
	}()

	mux := redcon.NewServeMux()
	handler.SetUpMux(mux)
	err = redcon.ListenAndServe(*addr,
		mux.ServeRESP,
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}
