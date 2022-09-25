package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/tidwall/redcon"
	"github.com/tikibu/rostore/store"
)

func mockStore(t *testing.T) *store.Store {
	records := store.MockRecords()

	// let's build a mock data jsonl file
	recordsBytes := store.MockJsonlBytes(records)

	//let's build an index
	index, err := store.BuildIndex(bytes.NewReader(recordsBytes))
	assert.NoError(t, err)
	//let's write an index
	indexBuf := bytes.Buffer{}
	err = index.WriteJsonl(&indexBuf)
	assert.NoError(t, err)

	//let's build a store
	store, err := store.NewStoreFromRecordsWithIndex(func() (io.ReadSeekCloser, error) {
		return store.NewReadSeekCloser(bytes.NewReader(recordsBytes)), nil
	},
		bytes.NewReader(indexBuf.Bytes()))

	return store
}

var next_port = 50000 - 1

func get_next_addr() string {
	next_port++
	return fmt.Sprintf(":%d", next_port)
}

func mockStoreAndClient(t *testing.T) (store *store.Store, rdb *redis.Client) {
	store = mockStore(t)

	handler := NewHandler(store)

	mux := redcon.NewServeMux()
	handler.SetUpMux(mux)

	addr := get_next_addr()
	go func() {
		_ = redcon.ListenAndServe(addr,
			mux.ServeRESP,
			func(conn redcon.Conn) bool {
				return true
			},
			func(conn redcon.Conn, err error) {
			},
		)
	}()

	time.Sleep(time.Millisecond * 10)

	rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return store, rdb
}

func TestInfoKeyspace(t *testing.T) {
	store, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	// Test INFO keyspace
	info, err := rdb.Info(ctx, "keyspace").Result()

	assert.NoError(t, err)
	assert.Contains(t, info, "Keyspace")
	assert.Contains(t, info, fmt.Sprintf("%d", store.GetLen()))
}

func TestInfoMem(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	// Test INFO memory
	info, err := rdb.Info(ctx, "memory").Result()

	assert.NoError(t, err)
	assert.Contains(t, info, "Memory")
	fmt.Println(info)
}

func TestScan(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	// Test Scan memory
	keys, cursor, err := rdb.Scan(ctx, 0, "", 20).Result()

	assert.NoError(t, err)
	assert.Equal(t, uint64(20), cursor)
	assert.Equal(t, 20, len(keys))
}

func TestHScan(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	// Test Scan memory
	keys, cursor, err := rdb.HScan(ctx, "key0:hash", 0, "", 20).Result()

	assert.NoError(t, err)
	assert.Equal(t, uint64(0), cursor)
	assert.Equal(t, 4, len(keys))
}

func TestHGetAll(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	// Test Scan memory
	kv, err := rdb.HGetAll(ctx, "key0:hash").Result()

	assert.NoError(t, err)
	assert.Equal(t, 2, len(kv))
}

func TestType(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	// Test INFO memory
	tp, err := rdb.Type(ctx, "key0:hash").Result()

	assert.NoError(t, err)
	fmt.Println(tp)
}

func TestMemoryUsage(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	usage, err := rdb.MemoryUsage(ctx, "key0:hash").Result()

	assert.NoError(t, err)
	assert.Greater(t, usage, int64(0))
	fmt.Println(usage)
}

func TestHlen(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	l, err := rdb.HLen(ctx, "key0:hash").Result()

	assert.NoError(t, err)
	assert.Equal(t, l, int64(2))
}

func TestLrange(t *testing.T) {
	_, rdb := mockStoreAndClient(t)

	ctx := context.Background()

	l, err := rdb.LRange(ctx, "key0:list", 0, 2).Result()

	assert.NoError(t, err)
	assert.Equal(t, len(l), 2)
}
