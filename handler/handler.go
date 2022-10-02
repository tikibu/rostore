package handler

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"text/template"

	"github.com/tidwall/redcon"
	"github.com/tikibu/rostore/store"
)

type Handler struct {
	Store   *store.Store
	Cursors map[string]map[int]string
}

func NewHandler(store *store.Store) *Handler {
	return &Handler{Store: store, Cursors: make(map[string]map[int]string)}
}

// Creates new handler with an empty Store
// I'm not sure what to do during initialization
func NewHandlerEmptyStore() *Handler {
	return NewHandler(store.NewEmptyStore())
}

func (h *Handler) SetNewStore(store *store.Store) {
	h.Store = store // race condition that does not matter
}

func (h *Handler) Detach(conn redcon.Conn, cmd redcon.Command) {
	detachedConn := conn.Detach()
	log.Printf("connection has been detached")
	go func(c redcon.DetachedConn) {
		defer c.Close()

		c.WriteString("OK")
		c.Flush()
	}(detachedConn)
}

func (h *Handler) Ping(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("PONG")
}

func (h *Handler) Quit(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteString("OK")
	conn.Close()
}

func (h *Handler) Set(conn redcon.Conn, cmd redcon.Command) {
	conn.WriteError("Unsupported command")
	/*
		if len(cmd.Args) != 3 {
			conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			return
		}

		h.itemsMux.Lock()
		h.items[string(cmd.Args[1])] = cmd.Args[2]
		h.itemsMux.Unlock()

		conn.WriteString("OK")
	*/
}
func printCmd(cmd redcon.Command) {
	fmt.Println("Command: ", string(cmd.Args[0]))
	for i, arg := range cmd.Args {
		fmt.Println("Arg ", i, ": ", string(arg))
	}
}

func (h *Handler) Get(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.StringType {
		conn.WriteError(fmt.Sprintf("ERR record for key %s is not a string", string(cmd.Args[1])))
		return
	}

	if record.StringRecord == nil {
		conn.WriteError(fmt.Sprintf("ERR record for key %s has no value", string(cmd.Args[1])))
		return
	}

	conn.WriteBulkString(record.StringRecord.Value)
}

func (h *Handler) Type(conn redcon.Conn, cmd redcon.Command) {
	printCmd(cmd)
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command: " + strconv.Itoa(len(cmd.Args)))
		return
	}

	record, err := h.Store.GetRecordIndex(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	conn.WriteBulkString(record.Type)
}

func (h *Handler) MemoryUsage(conn redcon.Conn, cmd redcon.Command) {
	printCmd(cmd)

	if len(cmd.Args) <= 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command: " + strconv.Itoa(len(cmd.Args)))
		return
	}

	if string(cmd.Args[1]) != "usage" {
		conn.WriteError("no usage keyword")
		return
	}

	record, err := h.Store.GetRecordIndex(string(cmd.Args[2]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	conn.WriteInt(record.Len)
}

func (h *Handler) HLen(conn redcon.Conn, cmd redcon.Command) {
	printCmd(cmd)

	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command: " + strconv.Itoa(len(cmd.Args)))
		return
	}

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.HashType {
		conn.WriteError("ERR wrong type to call hlen for")
		return
	}

	if record.HashRecord == nil {
		conn.WriteError("ERR record is empty")
		return
	}

	conn.WriteInt(len(record.HashRecord.Fields))

}

func (h *Handler) ZCard(conn redcon.Conn, cmd redcon.Command) {
	printCmd(cmd)

	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command: " + strconv.Itoa(len(cmd.Args)))
		return
	}

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.ZSetType {
		conn.WriteError("ERR wrong type to call zcard for")
		return
	}

	if record.OrdderSetRecord == nil {
		conn.WriteError("ERR record is empty")
		return
	}

	conn.WriteInt(len(record.OrdderSetRecord.Elements))

}

func (h *Handler) LLen(conn redcon.Conn, cmd redcon.Command) {
	printCmd(cmd)

	if len(cmd.Args) < 2 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command: " + strconv.Itoa(len(cmd.Args)))
		return
	}

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.ListType {
		conn.WriteError("ERR wrong type to call LLEN for")
		return
	}

	if record.ListRecord == nil {
		conn.WriteError("ERR record is empty")
		return
	}

	conn.WriteInt(len(record.ListRecord.Elements))

}

func (h *Handler) LRange(conn redcon.Conn, cmd redcon.Command) {
	printCmd(cmd)

	if len(cmd.Args) < 3 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command: " + strconv.Itoa(len(cmd.Args)))
		return
	}

	start, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		conn.WriteError("ERR occurred while parsing start ")
		return
	}
	stop, err := strconv.Atoi(string(cmd.Args[3]))
	if err != nil {
		conn.WriteError("ERR occurred while parsing stop ")
		return
	}

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.ListType {
		conn.WriteError("ERR wrong type to call LRANGE for")
		return
	}

	if record.ListRecord == nil {
		conn.WriteError("ERR record is empty")
		return
	}

	if start < 0 {
		start = len(record.ListRecord.Elements) + start
	}

	if start < 0 {
		start = 0
	}

	if stop < 0 {
		stop = len(record.ListRecord.Elements) + stop
	}

	if stop < 0 {
		stop = 0
	}

	if stop > len(record.ListRecord.Elements) {
		stop = len(record.ListRecord.Elements)
	}

	if start > len(record.ListRecord.Elements) {
		conn.WriteArray(0)
	}

	if start > stop {
		conn.WriteArray(0)
		return
	}

	fmt.Println("start", start, "stop", stop)
	conn.WriteArray(stop - start)
	for i := start; i < stop; i++ {
		conn.WriteBulkString(record.ListRecord.Elements[i])
	}

}

func (h *Handler) Delete(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("Unsupported command")
		return
	}
}

var section_sets = map[string][]string{
	"all":        {"server", "clients", "memory", "persistence", "stats", "replication", "cpu", "cluster", "keyspace"},
	"default":    {"server", "clients", "memory", "persistence", "stats", "replication", "cpu", "cluster", "keyspace"},
	"everything": {"server", "clients", "memory", "persistence", "stats", "replication", "cpu", "cluster", "keyspace"},
}

var info_template = map[string]*template.Template{
	"server":      template.Must(template.New("server").Parse("redis_version:4.0.1\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\n\r\nredis_build_id:f37081b32886670b\r\nredis_mode:standalone\r\nos:Darwin19.6.0x86_64\r\narch_bits:64\r\nmultiplexing_api:kqueue\r\natomicvar_api:atomic-builtin\r\ngcc_version:4.2.1\r\nprocess_id:1262\r\nrun_id:e37a3f975fa07aab297fa16ef1f572da3ab874b1\r\ntcp_port:6379\r\nuptime_in_seconds:3596475\r\nuptime_in_days:41\r\nhz:10\r\nlru_clock:3060158\r\nexecutable:/usr/local/opt/redis/bin/redis-server\r\nconfig_file:/usr/local/etc/redis.conf\r\n")),
	"clients":     template.Must(template.New("clients").Parse("connected_clients:1\r\nclient_recent_max_input_buffer:2\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:0\r\n")),
	"memory":      template.Must(template.New("memory").Parse("used_memory:{{.memory}}\r\nused_memory_human:{{.memory_human}}\r\nused_memory_rss:{{.memory}}\r\nused_memory_rss_human:{{.memory_human}}\r\nused_memory_peak:61684016\r\nused_memory_peak_human:58.83M\r\nused_memory_peak_perc:99.32%\r\nused_memory_overhead:31158374\r\nused_memory_startup:963824\r\nused_memory_dataset:30104714\r\nused_memory_dataset_perc:49.93%\r\ntotal_system_memory:17179869184\r\ntotal_system_memory_human:16.00G\r\nused_memory_lua:37888\r\nused_memory_lua_human:37.00K\r\nmaxmemory:0\r\nmaxmemory_human:0B\r\nmaxmemory_policy:noeviction\r\nmem_fragmentation_ratio:0.66\r\nmem_allocator:libc\r\nactive_defrag_running:0\r\nlazyfree_pending_objects:0\r\n")),
	"persistence": template.Must(template.New("persistence").Parse("loading:0\r\nrdb_changes_since_last_save:0\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:1597150009\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:-1\r\nrdb_current_bgsave_time_sec:-1\r\nrdb_last_cow_size:0\r\naof_enabled:0\r\naof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:ok\r\naof_last_write_status:ok\r\naof_last_cow_size:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\n")),
	"stats":       template.Must(template.New("stats").Parse("total_connections_received:1\r\ntotal_commands_processed:1\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:7\r\ntotal_net_output_bytes:3\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.00\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nexpired_stale_perc:0.00\r\nexpired_time_cap_reached_count:0\r\nevicted_keys:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\npubsub_channels:0\r\npubsub_patterns:0\r\nlatest_fork_usec:0\r\nmigrate_cached_sockets:0\r\nslave_expires_tracked_keys:0\r\nactive_defrag_hits:0\r\nactive_defrag_misses:0\r\nactive_defrag_key_hits:0\r\nactive_defrag_key_misses:0\r\ntracking_total_keys:0\r\ntracking_total_items:0\r\ntracking_total_prefixes:0\r\nunexpected_error_replies:0\r\n")),
	"replication": template.Must(template.New("replication").Parse("role:master\r\nconnected_slaves:0\r\nmaster_replid:0000000000000000000000000000000000000000\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n")),
	"cpu":         template.Must(template.New("cpu").Parse("used_cpu_sys:181.06\r\nused_cpu_user:91.95\r\nused_cpu_sys_children:0.00\r\nused_cpu_user_children:0.00\r\n")),
	"cluster":     template.Must(template.New("cluster").Parse("cluster_enabled:0\r\n")),
	"keyspace":    template.Must(template.New("keyspace").Parse("db0:keys={{.number_of_keys}},expires=0,avg_ttl=0\r\n")),
	"modules":     template.Must(template.New("modules").Parse("\r\n")),
}

func bytesToMegabytes(b uint64) float64 {
	return float64(b) / float64(1024*1024)
}

func (h *Handler) getTemplateWith() map[string]interface{} {

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	info := map[string]interface{}{
		"number_of_keys": h.Store.GetLen(),
		"memory":         m.TotalAlloc,
		"memory_human":   fmt.Sprintf("%.2fM", bytesToMegabytes(m.TotalAlloc)),
	}

	return info
}

func (h *Handler) Scan(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 1 {
		conn.WriteError("ERR too few parameters")
		return
	}
	printCmd(cmd)

	cursor := 0
	count := h.Store.GetLen()
	match := "*"

	for i := 1; i < len(cmd.Args); i++ {
		if i == 1 {
			var err error
			cursor, err = strconv.Atoi(string(cmd.Args[i]))
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR parsing cursor %s", err.Error()))
				return
			}
			continue
		}

		if string(cmd.Args[i]) == "count" {
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR parsing COUNT")
				return
			}
			var err error
			count, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR parsing COUNT, %s", err.Error()))
				return
			}
			continue
		}

		if string(cmd.Args[i]) == "match" {
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR parsing match")
				return
			}
			match = string(cmd.Args[i+1])
			continue
		}
	}
	keys, cursor, err := h.Store.ScanFields(cursor, count, match)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(2)
	conn.WriteString(strconv.Itoa(cursor))
	conn.WriteArray(len(keys))
	for _, indexRec := range keys {
		conn.WriteBulkString(indexRec.Key)
	}

}

func (h *Handler) HScan(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR too few parameters")
		return
	}
	printCmd(cmd)

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.HashType {
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	if record.HashRecord == nil {
		conn.WriteError("ERR empty hash record")
	}

	cursor := 0
	count := len(record.HashRecord.Fields)
	match := "*"

	for i := 1; i < len(cmd.Args); i++ {
		if i == 2 {
			var err error
			cursor, err = strconv.Atoi(string(cmd.Args[i]))
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR parsing cursor %s", err.Error()))
				return
			}
			continue
		}

		if string(cmd.Args[i]) == "count" {
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR parsing COUNT")
				return
			}
			var err error
			count, err = strconv.Atoi(string(cmd.Args[i+1]))
			if err != nil {
				conn.WriteError(fmt.Sprintf("ERR parsing COUNT, %s", err.Error()))
				return
			}
			continue
		}

		if string(cmd.Args[i]) == "match" {
			if i+1 >= len(cmd.Args) {
				conn.WriteError("ERR parsing match")
				return
			}
			match = string(cmd.Args[i+1])
			continue
		}
	}
	fields, cursor, err := record.HashRecord.ScanFields(cursor, count, match)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(2)
	conn.WriteString(strconv.Itoa(cursor))
	conn.WriteArray(len(fields))
	for _, field := range fields {
		conn.WriteBulkString(field)
	}

}

func (h *Handler) HGetAll(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) < 2 {
		conn.WriteError("ERR too few parameters")
		return
	}
	printCmd(cmd)

	record, err := h.Store.GetRecord(string(cmd.Args[1]))
	if err == store.ErrKeyNotFound {
		conn.WriteError("ERR no such key")
		return
	}

	if err != nil {
		conn.WriteError(fmt.Sprintf("ERR occurred while retrieving record for key %s", err.Error()))
		return
	}

	if record.Type != store.HashType {
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	if record.HashRecord == nil {
		conn.WriteError("ERR empty hash record")
	}

	count := len(record.HashRecord.Fields)

	fields, _, err := record.HashRecord.ScanFields(0, count, "*")
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(fields))
	for _, field := range fields {
		conn.WriteBulkString(field)
	}

}

func (h *Handler) Info(conn redcon.Conn, cmd redcon.Command) {
	sectionsToGive := []string{}
	if len(cmd.Args) == 1 {
		sectionsToGive = section_sets["default"]
	} else if len(cmd.Args) == 2 {
		// sections to give depend on first argument of the command
		sections, ok := section_sets[string(cmd.Args[1])]
		if ok {
			sectionsToGive = sections
		} else {
			sectionsToGive = []string{string(cmd.Args[1])}
		}
	} else {
		for _, arg := range cmd.Args[1:] {
			sectionsToGive = append(sectionsToGive, string(arg))
		}
	}

	fmt.Println("Info command from conn", conn.RemoteAddr(), "asking for sections", sectionsToGive)

	templateWith := h.getTemplateWith()

	// respond with bulk string,
	// that contains concatenated template-processed sections
	// which keys are in seconsToGive
	var b bytes.Buffer
	for _, section := range sectionsToGive {
		tpl, ok := info_template[string(section)]
		if ok {
			// make firt letter of section uppercase
			b.WriteString(fmt.Sprintf("# %s\r\n", strings.Title(section)))
			err := tpl.Execute(&b, templateWith)
			if err != nil {
				conn.WriteError("ERR while generating info" + err.Error())
				return
			}
			b.WriteString("\r\n")
		}
	}
	conn.WriteBulkString(b.String())
}

func (handler *Handler) SetUpMux(mux *redcon.ServeMux) {
	mux.HandleFunc("detach", handler.Detach)
	mux.HandleFunc("ping", handler.Ping)
	mux.HandleFunc("quit", handler.Quit)
	mux.HandleFunc("info", handler.Info)

	mux.HandleFunc("scan", handler.Scan)
	mux.HandleFunc("type", handler.Type)
	mux.HandleFunc("memory", handler.MemoryUsage)

	mux.HandleFunc("get", handler.Get)
	// hash specific commands
	mux.HandleFunc("hlen", handler.HLen)
	mux.HandleFunc("hscan", handler.HScan)
	mux.HandleFunc("hgetall", handler.HGetAll)

	// list specific commands
	mux.HandleFunc("llen", handler.LLen)
	mux.HandleFunc("lrange", handler.LRange)

	// zset specific commands
	mux.HandleFunc("zcard", handler.ZCard)
}
