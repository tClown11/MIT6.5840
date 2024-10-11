package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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

// for sort by key
type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

// finalizeReduceFile atomically rename temporary reduce file to complete reduce task file
func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}

// get name of the intermediate file, given the map and reduce task numbers
func getIntermediateFile(mapTaskN int, redTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
}

// finalizeIntermediateFile atomically rename temporary intermediate files to a completed intermediate task file
func finalizeIntermediateFile(tmpFile string, mapTaskN int, redTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, redTaskN)
	os.Rename(tmpFile, finalFile)
}

// impl  of map task
func performMap(fileName string, taskNum int, nReduceTask int, mapf func(string, string) []KeyValue) {
	// read contents to map
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("map cannot open: %s req: %+v\n", fileName, taskNum)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read: %s \n", file.Name())
	}
	file.Close()

	// apply map func to contents of file and collect the set of key-value pairs.
	kva := mapf(fileName, string(content))

	// create temporary files and encoders each file
	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}

	for i := 0; i < nReduceTask; i++ {
		tmpFile, err := os.CreateTemp("", "")
		if err != nil {
			log.Fatalf("cannot open tempfile")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilename := tmpFile.Name()
		tmpFilenames = append(tmpFilenames, tmpFilename)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	// write output keys to appropriate (temporary!) intermediate files
	// using the provided ihash function
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduceTask
		encoders[r].Encode(&kv)
	}

	for _, f := range tmpFiles {
		f.Close()
	}

	// atomically rename temp files to final intermediate files
	for i := 0; i < nReduceTask; i++ {
		finalizeIntermediateFile(tmpFilenames[i], taskNum, i)
	}
}

// impl of reduce task
func performReduce(taskNum int, nMapTasks int, reducef func(string, []string) string) {
	// get all intermediate files corresponding to this reduce task, and
	// collect the coresponding key-value pairs.
	kva := []KeyValue{}

	for m := 0; m < nMapTasks; m++ {
		iFilename := getIntermediateFile(m, taskNum)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("reduce cannot open: %s \n", iFilename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// sort the keys
	sort.Sort(ByKey(kva))

	// get temporary reduce file to write values
	tmpFile, err := os.CreateTemp("", "")
	if err != nil {
		log.Fatalf("cannot open tempfile")
	}
	tmpFilename := tmpFile.Name()

	// apply reduce function once to all value of the same key
	key_begin := 0

	for key_begin < len(kva) {
		key_end := key_begin + 1
		// this loop finds all values with same keys --- they are grouped
		// together because the keys are sorted
		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}

		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[key_begin].Key, values)

		// write output to reduce task tmp file
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)

		// go to next key
		key_begin = key_end
	}

	// atomically rename reduce file to final reduce file
	finalizeReduceFile(tmpFilename, taskNum)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		resp := getTask()
		if resp.TaskType == None {
			//log.Printf("not found task, sleep 1 second")
			time.Sleep(time.Second)
			continue
		}

		switch resp.TaskType {
		case Map:
			// exec map func
			performMap(resp.MapFile, resp.TaskNum, resp.NReduceTasks, mapf)
		case Reduce:
			// exec reduce func
			performReduce(resp.TaskNum, resp.NMapTasks, reducef)
		case Complete:
			// task complete
			return
		default:
			fmt.Errorf("Bad task type: %v \n", resp.TaskType)
		}
		// tell coordinator that we're done
		call("Coordinator.FinishTask", &FinishArgs{
			TaskType: resp.TaskType,
			TaskNum:  resp.TaskNum,
		}, &FinishReply{})
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func getTask() *GetTaskReply {
	var res = &GetTaskReply{}
	call("Coordinator.GetTask", &GetTaskArgs{}, &res)
	return res
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err)
	return false
}
