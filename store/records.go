package store

import (
	"encoding/json"
	"hash/fnv"

	"github.com/tidwall/match"
)

type StringRecord struct {
	Value string `json:"value"`
}

type HashRecord struct {
	Fields map[string]string `json:"fields"`
	//let's go w/o this. This going to be slower, but this waves a requirement for syncing this
	//OrderedFields []string          `json:"ordered_fields,omitempty"`
}

func makeHash(s string) int {
	hash := fnv.New32a()
	hash.Write([]byte(s))
	return int(hash.Sum32())
}

func (hr *HashRecord) ScanFields(cursor int, count int, pattern string) (fields []string, last int, err error) {
	foundIt := cursor == 0
	stoppedEarly := false
	last = 0
	for key, value := range hr.Fields {
		// this is quite inefficient for big hashes, but this
		// waves a requirement for syncing & keeping a state for the cursor
		if !foundIt {
			last = makeHash(key)
			if last == cursor {
				foundIt = true
			}
			continue
		}

		if pattern != "" && !match.Match(key, pattern) {
			continue
		}

		fields = append(fields, key)
		fields = append(fields, value)
		if len(fields) >= 2*count {
			stoppedEarly = true
			break
		}
	}
	if !stoppedEarly {
		last = 0
	}
	return fields, last, nil
}

type ListRecord struct {
	Elements []string `json:"elements"`
}

type OrderedSetElement struct {
	Value string  `json:"value"`
	Score float64 `json:"score"`
}
type OrderedSetRecord struct {
	Elements []OrderedSetElement `json:"elements"`
}

// string enum
const (
	StringType string = "string"
	HashType          = "hash"
	ListType          = "list"
	SetType           = "set"
	ZSetType          = "zset"
)

type Record struct {
	Key  string `json:"key"`
	Type string `json:"type"`

	StringRecord    *StringRecord     `json:"string_record,omitempty"`
	HashRecord      *HashRecord       `json:"hash_record,omitempty"`
	ListRecord      *ListRecord       `json:"list_record,omitempty"`
	OrdderSetRecord *OrderedSetRecord `json:"ordered_set_record,omitempty"`
}

func (r *Record) String() string {
	bts, _ := json.Marshal(r)
	return string(bts)
}
