package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerId := generateWorkerID()

	for {
		time.Sleep(5 * time.Second)
		args := RequestArgs{WorkerID: workerId}
		reply := ReplyArgs{}

		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			time.Sleep(3 * time.Second)
			continue
		}

		switch reply.Task.Phase {
		case MapPhase:
			doMap(mapf, &args, &reply)
		case ReducePhase:
			doReduce(reducef, &args, &reply)
		}
	}
}

func doMap(mapf func(string, string) []KeyValue, args *RequestArgs, reply *ReplyArgs) {
	file, err := os.Open(reply.Task.File)
	if err != nil {
		fmt.Printf("cannot open %v\n", reply.Task.File)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("cannot read %v\n", reply.Task.File)
		return
	}
	file.Close()

	kva := mapf(reply.Task.File, string(content))

	intermediateFiles := make([]*bufio.Writer, reply.NReduce)
	base := filepath.Base(reply.Task.File)               // remove "../data/"
	name := strings.TrimSuffix(base, filepath.Ext(base)) // remove ".txt"
	for i := range intermediateFiles {
		intermediateFileName := fmt.Sprintf("m-%s-%d.txt", name, i)
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			fmt.Printf("cannot open %v\n", intermediateFileName)
			return
		}
		defer intermediateFile.Close()
		intermediateFiles[i] = bufio.NewWriter(intermediateFile)
	}

	for _, kv := range kva {
		hashVal := ihash(kv.Key) % reply.NReduce
		fmt.Fprintf(intermediateFiles[hashVal], "%v|%v\n", kv.Key, kv.Value)
	}

	for _, writer := range intermediateFiles {
		if err := writer.Flush(); err != nil {
			fmt.Printf("cannot flush writer: %v\n", err)
			return
		}
	}

	taskDoneReportArgs := ReportTaskDoneArgs{WorkerID: args.WorkerID, TaskID: reply.Task.TaskID}
	taskDoneReportReply := ReportTaskDoneReply{}

	ok := call("Coordinator.ReportTaskDone", &taskDoneReportArgs, &taskDoneReportReply)
	if !ok {
		fmt.Print("Failed to call Coordinator.ReportTaskDone, retrying...\n")
		return
	}
}

func doReduce(reducef func(string, []string) string, args *RequestArgs, reply *ReplyArgs) {
	intermediate := make(map[string][]string)

	intermediateFileName := strings.Replace(reply.Task.File, "out", "*", -1)
	intermediateFileName = strings.Replace(intermediateFileName, "mr", "m", -1)
	files, err := filepath.Glob(intermediateFileName)
	if err != nil {
		fmt.Printf("cannot find files with pattern %v\n", intermediateFileName)
		return
	}

	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("cannot open %v\n", fileName)
			return
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			kv := strings.Split(line, "|")
			if len(kv) == 2 {
				key := kv[0]
				value := kv[1]
				intermediate[key] = append(intermediate[key], value)
			}
		}
		if err := scanner.Err(); err != nil {
			fmt.Printf("error reading file %v: %v\n", fileName, err)
			return
		}
	}

	ofileName := fmt.Sprintf("%v", reply.Task.File)
	ofileInfo, err := os.Stat(ofileName)
	if err == nil && ofileInfo.Size() > 0 {
		// fmt.Printf("Output file %v already contains data, skipping...\n", ofileName)
		return
	}

	ofile, err := os.OpenFile(ofileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Printf("cannot open %v\n", intermediateFileName)
		return
	}

	for key, values := range intermediate {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}

	taskDoneReportArgs := ReportTaskDoneArgs{WorkerID: args.WorkerID, TaskID: reply.Task.TaskID}
	taskDoneReportReply := ReportTaskDoneReply{}

	ok := call("Coordinator.ReportTaskDone", &taskDoneReportArgs, &taskDoneReportReply)
	if !ok {
		fmt.Print("Failed to call Coordinator.ReportTaskDone, retrying...\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

func generateWorkerID() string {
	rand.Seed(time.Now().UnixNano())
	id := fmt.Sprintf("%d-%d", time.Now().UnixNano(), rand.Int())
	return id
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}
