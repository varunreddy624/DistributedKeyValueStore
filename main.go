package main

import (
	"os"
	"fmt"
	"strings"
	"strconv"

	"bytes"
	"io/ioutil"
	"net/http"
	"log"
	"encoding/json"
	"github.com/gorilla/mux"
	
	// "time"
	// "context"
	// "ShardedKeyValueStore/KV"
)

type entry struct {
	Key   string
	Value string
}

var angleToGid [360]int
var gIDtoAddress = make(map[int]string)

func SendConfig(url string){
	jsonBody := map[string]interface{}{
		"angleToGid":angleToGid,
		"gIDtoAddress":gIDtoAddress,
	}
	jsonStr,_ := json.Marshal(jsonBody)

	req, err := http.NewRequest("POST", url+"migrate", bytes.NewBuffer(jsonStr))

	if err != nil{
		panic(err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
}

func PutToCluster(url string, key string, value string){
	jsonBody := fmt.Sprintf(`{"Key":"%s", "Value":"%s"}`, key, value)

    var jsonStr = []byte(jsonBody)

    req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
    req.Header.Set("Content-Type", "application/json")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        panic(err)
    }
    defer resp.Body.Close()
}

func GetFromCluster(url string, key string) string {
	resp, err := http.Get(url+key)
    if err != nil {
        log.Fatalln(err)
    }

	if resp != nil {
        body, err := ioutil.ReadAll(resp.Body)
        if err != nil {
            log.Fatalln(err)
        }

        result := string(body)

        resp.Body.Close()

		return result
    } else {
        panic(resp)
    }
}


func GetClusterAddressFromHash(hash int) string{
	for i := 0; i<360; i++{
		currentAngle := (hash-i+360)%360
		if angleToGid[currentAngle] != -1{
			clusterAddress := gIDtoAddress[angleToGid[currentAngle]]
			clusterAddress = "http://127.0.0.1:"+clusterAddress+"/"
			fmt.Println(hash, currentAngle)
			return clusterAddress
		}
	}
	return "no hash found"
}

func main(){

	for i :=0; i<360; i++{
		angleToGid[i] = -1
	}

	port := os.Args[1]

	for _,i :=  range os.Args[2:]{
		gidAndPort := strings.Split(i,":")
		gid := gidAndPort[0]
		port := gidAndPort[1]
		angle,_ := strconv.Atoi(gid)
		angleToGid[angle%360] = angle
		gIDtoAddress[angle] = port
	}
	// fmt.Println(angleToGid, gIDtoAddress)

	router := mux.NewRouter()
	router.HandleFunc("/", http.HandlerFunc(save)).Methods("PUT", "POST")
	router.HandleFunc("/{key}", http.HandlerFunc(get)).Methods("GET")

	router.HandleFunc("/join/{gid}/{clusterAddr}",http.HandlerFunc(join)).Methods("GET")
	router.HandleFunc("/leave/{gid}",http.HandlerFunc(leave)).Methods("GET")

	router.HandleFunc("/changeClusterRoot/{gid}/{newAddress}",http.HandlerFunc(changeClusterRoot)).Methods("GET")

	if err := http.ListenAndServe(port, router); err != nil {
		log.Fatal(err)
	}
}

func save(w http.ResponseWriter, r *http.Request) {
	var e entry

	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&e)

	if err != nil {
		return
	}

	replicaGroup := GetClusterAddressFromHash(hashingFunc(e.Key))
	
	PutToCluster(replicaGroup, e.Key, e.Value)

	w.WriteHeader(http.StatusNoContent)
}

func get(w http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]

	replicaGroup :=  GetClusterAddressFromHash(hashingFunc(key))

	value := GetFromCluster(replicaGroup,key)

	w.Write([]byte(value))
}

func join(w http.ResponseWriter, r *http.Request){
	gid := mux.Vars(r)["gid"]
	clusterAddr := mux.Vars(r)["clusterAddr"]

	nodeAngle,_ := strconv.Atoi(gid)

	nearestCluster := GetClusterAddressFromHash(nodeAngle%360)

	angleToGid[nodeAngle%360] = nodeAngle
	gIDtoAddress[nodeAngle] = clusterAddr
	

	SendConfig(nearestCluster)

	w.WriteHeader(http.StatusNoContent)
}

func leave(w http.ResponseWriter, r *http.Request){
	gid := mux.Vars(r)["gid"]

	nodeAngle,_ := strconv.Atoi(gid)

	nearestCluster := GetClusterAddressFromHash(nodeAngle%360)

	angleToGid[nodeAngle%360] = -1
	delete(gIDtoAddress,nodeAngle)

	SendConfig(nearestCluster)

	w.WriteHeader(http.StatusNoContent)
}

func changeClusterRoot(w http.ResponseWriter, r *http.Request){
	gid := mux.Vars(r)["gid"]
	newAddress := mux.Vars(r)["newAddress"]

	nodeAngle,_ := strconv.Atoi(gid)

	fmt.Println(gIDtoAddress)
	gIDtoAddress[nodeAngle] = newAddress
	fmt.Println(gIDtoAddress)

	w.WriteHeader(http.StatusNoContent)
}

func hashingFunc(key string) int {
	asciiStr := []rune(key)
	var summnation int
	for _,ascii:= range asciiStr{
		summnation = summnation + int(ascii)
	}
	return summnation%360
}