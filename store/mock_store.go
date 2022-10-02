package store

import (
	"bytes"
	"encoding/json"
	"fmt"
)

func MockRecords() (records []Record) {
	for i := 0; i < 10; i++ {
		records = append(records, Record{
			Key:  fmt.Sprintf("key%d:string", i),
			Type: StringType,
			StringRecord: &StringRecord{
				Value: "value1",
			},
		})

		records = append(records, Record{
			Key:  fmt.Sprintf("key%d:hash", i),
			Type: HashType,
			HashRecord: &HashRecord{
				Fields: map[string]string{
					fmt.Sprintf("field%d:1", i): "value1",
					fmt.Sprintf("field%d:2", i): "value1",
				},
			},
		})

		records = append(records, Record{
			Key:  fmt.Sprintf("key%d:list", i),
			Type: ListType,
			ListRecord: &ListRecord{
				Elements: []string{
					fmt.Sprintf("element%d:1", i),
					fmt.Sprintf("element%d:2", i),
				},
			},
		})

		records = append(records, Record{
			Key:  fmt.Sprintf("key%d:zset", i),
			Type: ZSetType,
			OrdderSetRecord: &OrderedSetRecord{
				Elements: []OrderedSetElement{
					{
						Value: fmt.Sprintf("key%d:zset:1", i),
						Score: 1.0,
					},
					{
						Value: fmt.Sprintf("key%d:zset:2", i),
						Score: 2.0,
					},
				},
			},
		})

	}
	return records
}

func MockJsonlBytes(records []Record) []byte {
	// serializes the records to jsonl into bytes buffer
	var b bytes.Buffer
	m := json.NewEncoder(&b)
	for _, record := range records {
		m.Encode(record)
	}
	return b.Bytes()
}
