package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"bytes"

	"github.com/gorilla/mux"
	"github.com/shaj13/raft"
	"github.com/shaj13/raft/transport"
	"github.com/shaj13/raft/transport/raftgrpc"
	"google.golang.org/grpc"
)

var api, sc , gid *string

func main() {

	addr := flag.String("raft", "", "raft server address")
	join := flag.String("join", "", "join cluster address")
	api = flag.String("api", "", "api server address")
	state := flag.String("state_dir", "", "raft state directory (WAL, Snapshots)")
	sc = flag.String("shrdctlraddr", "", "ip and port of shard controller")
	gid = flag.String("group_id", "", "group id to which current node belongs")

	flag.Parse()

	fmt.Println(*addr, *join, *api, *state, *sc, *gid)

	router := mux.NewRouter()
	router.HandleFunc("/", http.HandlerFunc(save)).Methods("PUT", "POST")
	router.HandleFunc("/{key}", http.HandlerFunc(get)).Methods("GET")
	router.HandleFunc("/mgmt/nodes", http.HandlerFunc(nodes)).Methods("GET")
	router.HandleFunc("/mgmt/nodes/{id}", http.HandlerFunc(removeNode)).Methods("DELETE")

	router.HandleFunc("/migrate", http.HandlerFunc(migrate)).Methods("PUT", "POST")
	router.HandleFunc("/receive", http.HandlerFunc(receive)).Methods("PUT", "POST")

	var (
		opts      []raft.Option
		startOpts []raft.StartOption
	)

	startOpts = append(startOpts, raft.WithAddress(*addr))
	opts = append(opts, raft.WithStateDIR(*state))
	if *join != "" {
		opt := raft.WithFallback(
			raft.WithJoin(*join, time.Second),
			raft.WithRestart(),
		)
		startOpts = append(startOpts, opt)
	} else {
		opt := raft.WithFallback(
			raft.WithInitCluster(),
			raft.WithRestart(),
		)
		startOpts = append(startOpts, opt)
	}

	raftgrpc.Register(
		raftgrpc.WithDialOptions(grpc.WithInsecure()),
	)
	fsm = newstateMachine()
	node = raft.NewNode(fsm, transport.GRPC, opts...)
	raftServer := grpc.NewServer()
	raftgrpc.RegisterHandler(raftServer, node.Handler())

	go func() {
		lis, err := net.Listen("tcp", *addr)
		if err != nil {
			log.Fatal(err)
		}

		err = raftServer.Serve(lis)
		if err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		err := node.Start(startOpts...)
		if err != nil && err != raft.ErrNodeStopped {
			log.Fatal(err)
		}
	}()

	if err := http.ListenAndServe(*api, router); err != nil {
		log.Fatal(err)
	}

	// fmt.Println("node at %s started", addr)
}

func newstateMachine() *stateMachine {
	return &stateMachine{
		kv: make(map[string]string),
	}
}

type stateMachine struct {
	mu sync.Mutex
	kv map[string]string
}

func (s *stateMachine) Apply(data []byte) {
	var e entry
	if err := json.Unmarshal(data, &e); err != nil {
		log.Println("unable to Unmarshal entry", err)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv[e.Key] = e.Value
}

func (s *stateMachine) Snapshot() (io.ReadCloser, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf, err := json.Marshal(&s.kv)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(string(buf))), nil
}

func (s *stateMachine) Restore(r io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}

	err = json.Unmarshal(buf, &s.kv)
	if err != nil {
		return err
	}

	return r.Close()
}

func (s *stateMachine) Read(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.kv[key]
}

type entry struct {
	Key   string
	Value string
}

var (
	node *raft.Node
	fsm  *stateMachine
)

func get(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()

	if err := node.LinearizableRead(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	value := fsm.Read(key)
	w.Write([]byte(value))
}

func nodes(w http.ResponseWriter, r *http.Request) {
	raws := []raft.RawMember{}
	membs := node.Members()
	for _, m := range membs {
		raws = append(raws, m.Raw())
	}

	buf, err := json.Marshal(raws)
	if err != nil {
		panic(err)
	}

	w.Write(buf)
}

func removeNode(w http.ResponseWriter, r *http.Request) {
	sid := mux.Vars(r)["id"]
	id, err := strconv.ParseUint(sid, 0, 64)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()

	if node.Leader() != id{
		// non leader delete, need not inform shard controller
		if err := node.RemoveMember(ctx, id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		// leader delete, inform shard controller

		// first transfer leadership
		flag := false
		members := node.Members()
		for _,member := range members{
			if member.ID() != id && member.Type() != 1{
				flag = true
				err = node.TransferLeadership(ctx, member.ID())
				if err != nil{
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return					
				}
				break
			}
		}	
		shardcontrollerFullAddress := GetFullAddr(*sc)	
		if flag {
			// enough members exists
			if err := node.RemoveMember(ctx, id); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			// send transferOwnership to shardcontroller
			resp, err := http.Get(shardcontrollerFullAddress+"/changeClusterRoot/"+(*gid)+"/"+(*api)[1:])
			if err != nil {
				// Handle the error
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()
		}else{
			// send leave to shardcontroller
			resp, err := http.Get(shardcontrollerFullAddress+"/leave/"+(*gid))
			if err != nil {
				// Handle the error
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer resp.Body.Close()
		}
	}

	// fmt.Println(node.Leader())
	w.WriteHeader(http.StatusNoContent)
}

func save(w http.ResponseWriter, r *http.Request) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := json.Unmarshal(buf, new(entry)); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()

	if err := node.Replicate(ctx, buf); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type configuration struct {
	AngleToGid []int `json:"angleToGid"`
	GIDtoAddress map[int]string `json:gIDtoAddress`
}

type ReceiveMap struct {
	Map []entry `json:"map"`
}

func GetClusterAddressFromHash(conf configuration, hash int) string {
	for i := 0; i < 360; i++ {
		currentAngle := (hash - i + 360) % 360
		if conf.AngleToGid[currentAngle] != -1 {
			clusterAddress := "http://127.0.0.1:" + conf.GIDtoAddress[conf.AngleToGid[currentAngle]] + "/"
			fmt.Println(hash, currentAngle)
			return clusterAddress
		}
	}
	return "no hash found"
}

func hashingFunc(key string) int {
	asciiStr := []rune(key)
	var summnation int
	for _, ascii := range asciiStr {
		summnation = summnation + int(ascii)
	}
	return summnation % 360
}

func GetFullAddr(port string) string {
	return "http://127.0.0.1" + port + "/"
}

func migrate(w http.ResponseWriter, r *http.Request) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conf := configuration{}
	err = json.Unmarshal(buf, &conf)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	apiFullAddr := GetFullAddr(*api)

	fmt.Println(conf)

	var targetClusterAddr string
	var targetMap ReceiveMap
	var m = []entry{}

	for k, v := range fsm.kv {
		fmt.Println(k, v)
		clusterAdd := GetClusterAddressFromHash(conf, hashingFunc(k))
		if v!= "" && clusterAdd != apiFullAddr {
			targetClusterAddr = clusterAdd
			e := entry{
				Key:    k,
				Value: v,
			}
			m = append(m, e)
			// targetMap[k] = v
		}
	}
	targetMap.Map = m

	// fmt.Println(targetClusterAddr, targetMap)
	sendToTarget(targetClusterAddr, targetMap)

	// remove the key values pairs from current node and

	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()

	for _, elem := range m {
		elem.Value = ""
		
		elemInByteArr, err := json.Marshal(elem)
		if err != nil {
			panic(err)
		}

		if err := node.Replicate(ctx, elemInByteArr); err != nil {
			fmt.Println(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func sendToTarget(url string, targetMap ReceiveMap) {

	fmt.Println(targetMap)

	targetMapByteArr, err := json.Marshal(targetMap)
	if err != nil {
		panic(err)
	}

	req, err := http.NewRequest("POST", url+"receive", bytes.NewBuffer(targetMapByteArr))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
	    panic(err)
	}
	defer resp.Body.Close()
}

func receive(w http.ResponseWriter, r *http.Request) {
	buf, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	newMap := ReceiveMap{}
	err = json.Unmarshal(buf, &newMap)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Println(newMap.Map)

	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()

	for _, elem := range newMap.Map {
		elemInByteArr, err := json.Marshal(elem)
		if err != nil {
			panic(err)
		}
		if err := node.Replicate(ctx, elemInByteArr); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}
