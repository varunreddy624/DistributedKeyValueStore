package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"log"
	"os"
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

func SendConfig(url string){
	jsonBody := map[string]interface{}{
		"config":angles,
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

var angles [360]string


func GetClusterAddressFromHash(hash int) string{
	for i := 0; i<360; i++{
		currentAngle := (hash-i+360)%360
		if angles[currentAngle] != ""{
			clusterAddress := "http://127.0.0.1"+angles[currentAngle]+"/"
			fmt.Println(hash, currentAngle)
			return clusterAddress
		}
	}
	return "no hash found"
}

func main(){

	port := os.Args[1]

	for _,i :=  range os.Args[2:]{
		angle := hashingFunc(i)
		angles[angle] = i
		fmt.Println(i,angle)
	}

	router := mux.NewRouter()
	router.HandleFunc("/", http.HandlerFunc(save)).Methods("PUT", "POST")
	router.HandleFunc("/{key}", http.HandlerFunc(get)).Methods("GET")

	router.HandleFunc("/join/{node}",http.HandlerFunc(join)).Methods("GET")
	router.HandleFunc("/leave/{node}",http.HandlerFunc(leave)).Methods("GET")

	router.HandleFunc("/changeClusterRoot/{oldNode}/{newNode}",http.HandlerFunc(changeClusterRoot)).Methods("GET")

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
	node := mux.Vars(r)["node"]
	nodeAngle := hashingFunc(node)

	nearestCluster := GetClusterAddressFromHash(nodeAngle)

	angles[nodeAngle] = node
	

	SendConfig(nearestCluster)

	// fmt.Println(node,nodeAngle)

	w.WriteHeader(http.StatusNoContent)
}

func leave(w http.ResponseWriter, r *http.Request){
	node := mux.Vars(r)["node"]
	nodeAngle := hashingFunc(node)

	nearestCluster := GetClusterAddressFromHash(nodeAngle)

	angles[nodeAngle] = ""

	SendConfig(nearestCluster)

	fmt.Println(node,nodeAngle)

	w.WriteHeader(http.StatusNoContent)
}

func changeClusterRoot(w http.ResponseWriter, r *http.Request){
	fmt.Println("here")
	
	oldNode := mux.Vars(r)["oldNode"]
	newNode := mux.Vars(r)["newNode"]

	fmt.Println(oldNode,newNode)

	for index,elem := range angles{
		if elem == oldNode{
			angles[index] = newNode
		}
	}

	fmt.Println(angles)

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

// TODO: cluster master node failure