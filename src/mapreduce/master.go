package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}


// Credit Disclaimer: 
// I read the code of the Github users below 
// and finally got a sense of what we are doing here.
// the solution is more or less inspired by their work.
//
// tushar00jain
// https://github.com/tushar00jain/distribute/tree/master/mapreduce
// 
// pranjalv123
// https://github.com/pranjalv123/mexos/tree/master/labs/cjoseph/mapreduce



// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here

	// Start all workers
	go RunWorkers(mr)

	// Add all reduce work to workToDo channel
	go AddJobs(mr, Map)

	for i := 0; i < mr.nMap; i++ {
		<-mr.workReplyChannel
	}

	// Add all reduce work to workToDo channel
	go AddJobs(mr, Reduce)

	for i := 0; i < mr.nReduce; i++ {
		<-mr.workReplyChannel
	}

	// all map and reduce work is finished
	mr.mapReduceFinished <- true

	return mr.KillWorkers()
}

func AddJobs(mr *MapReduce, stage JobType){
	mrNumber := 0
	numOther := 0
	switch stage {
	case Map:
		mrNumber = mr.nReduce
		numOther = mr.nMap
	case Reduce:
		mrNumber = mr.nMap
		numOther = mr.nReduce
	}
	for i := 0; i < numOther; i++ {
		args := &DoJobArgs{mr.file, stage, i, mrNumber}
		mr.workToDoChannel <- args
	}
}

func StartWorker(mr *MapReduce, worker string) {
	workToDo := <-mr.workToDoChannel
	var jobReply DoJobReply
	ok := call(worker, "Worker.DoJob", workToDo, &jobReply)
	if ok {
		mr.workReplyChannel <- true
		//put the worker into registerChannel to wait for the next job
		mr.registerChannel <- worker
	} else {
		fmt.Printf("DoJob: RPC %s Job error\n", worker)
		delete(mr.Workers, worker)
		// put work back to the workToDoChannel
		mr.workToDoChannel <- workToDo
	}
}


func RunWorkers(mr *MapReduce){
	for {
			select {
			case worker := <-mr.registerChannel:
				mr.Workers[worker] = &WorkerInfo{worker}
				// get the worker started with a job
				go	StartWorker(mr, worker)
			case <- mr.mapReduceFinished:
				break
			}
		}
}
