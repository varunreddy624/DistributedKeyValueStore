package src

import (
	"github.com/varunreddy624/DistributedKeyValueStore/src/master"
	"github.com/varunreddy624/DistributedKeyValueStore/src/worker"
	"sync"
)

func main() {
    var wg sync.WaitGroup
    wg.Add(1)

    masterSrv := master.StartMasterSrv(8080)
	workerSrv1 := worker.StartWorkerSrv(8081,1)
	workerSrv2 :=worker.StartWorkerSrv(8082,2)
	workerSrv3 :=worker.StartWorkerSrv(8083,3)
	worker.StartWorkerCli(masterSrv,[]string{workerSrv1,workerSrv2,workerSrv3})
	master.StartMasterCli()

	wg.Wait()
}
