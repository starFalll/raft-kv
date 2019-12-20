package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//Deal one of M map tasks to R intermediate files
func DealMap(mapf func(string, string) []KeyValue,
	mapstruct MapReply) map[int][]string {
	file, err := os.Open(mapstruct.Filename)
	if err != nil {
		log.Fatalf("cannot open:%v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read:%v", mapstruct.Filename)
	}
	file.Close()

	//intermediate k-v pairs
	kva := mapf(mapstruct.Filename, string(content))

	//make same hash code to one map array
	hashmaps := make(map[int][]KeyValue)
	for _, kv := range kva {

		r := ihash(kv.Key) % mapstruct.NReduce
		hashmaps[r] = append(hashmaps[r], kv)
	}

	//hashcode -> filenames (there have R hashcodes)
	interfiles := make(map[int][]string)
	for k, v := range hashmaps {
		filename := ("mr-" + strconv.Itoa(mapstruct.Tasknumber) + "-" + strconv.Itoa(k+1))
		os.Remove(filename)
		file, er := os.Create(filename)
		if er != nil {
			log.Fatalf("cannot create:%v", filename)
		}
		//write to intermediate file
		enc := json.NewEncoder(file)
		for _, kv := range v {
			er = enc.Encode(&kv)
			if er != nil {
				log.Fatalf("cannot encode:%v", filename)
			}
		}
		interfiles[k] = append(interfiles[k], filename)
		file.Close()
	}

	return interfiles
}

func DealReduce(reducef func(string, []string) string,
	reducestruct ReduceReply) string {
	var kva []KeyValue
	for _, filename := range reducestruct.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open:%v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	log.Printf("DealReduce:%v\n", reducestruct.Files)
	sort.Sort(ByKey(kva))
	oname := ("mr-out-" + strconv.Itoa(reducestruct.Tasknumber+1))
	os.Remove(oname)
	ofile, _ := os.Create(oname)
	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-X.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()

	return oname
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	//
	//Map Begin
	//
	for {
		mapargs := MapTaskArgs{}
		mapargs.Maptask = true
		mapreply := MapReply{}
		//request map task
		call("Master.AssignMap", &mapargs, &mapreply)
		//log.Printf("Worker(mapf func(string, string) res:%v flag:%v\n", res, mapreply.Flag)
		if mapreply.Flag == 1 {
			//store all intermediate files
			interfiles := DealMap(mapf, mapreply)

			endmapargs := EndMapArgs{mapreply.Tasknumber, interfiles}
			endmapreply := EndMapReply{}

			//upload intermediate files name
			res := call("Master.CommitMediateFiles", &endmapargs, &endmapreply)
			log.Printf("Master.CommitMediateFiles res:%v \n", res)
			if endmapreply.Err != nil {
				log.Fatalf("Commit intermediate files failue, tasknum:%v", mapreply.Tasknumber)
			}
		} else if mapreply.Flag == 2 {
			time.Sleep(10 * time.Millisecond)
		} else if mapreply.Flag == 3 {
			log.Printf("all map tasks end,begin to reduce:%v\n", mapreply.Flag)
			break
		} else {
			log.Printf("Request for map task failue:%v\n", mapreply.Flag)
			break
		}
	}
	//
	//Map End
	//

	//
	//Reduce Begin
	//

	for {
		reduceargs := MapTaskArgs{false}
		reducereply := ReduceReply{}
		//request reduce task
		call("Master.AssignReduce", &reduceargs, &reducereply)
		//log.Printf("Call Master.AssignReduce res:%v, flag:%v\n", res, reducereply.Flag)
		if reducereply.Flag == 1 {

			filename := DealReduce(reducef, reducereply)
			reportreduceargs := ReportReduceArgs{reducereply.Tasknumber, filename}
			reportreducereply := ReportReduceReply{}
			//upload mr-out-X file name
			call("Master.CommitFinalFiles", &reportreduceargs, &reportreducereply)
			if reportreducereply.Err != nil {
				log.Fatalf("Commit final files failue, tasknum:%v", reducereply.Tasknumber)
			}

		} else if reducereply.Flag == 2 {
			time.Sleep(10 * time.Millisecond)
		} else if reducereply.Flag == 3 {
			log.Printf("All reduce tasks end\n")
			break
		} else {
			log.Printf("Request for reduce task failue :%v\n", reducereply.Flag)
			break
		}
	}
	//
	//Reduce End
	//

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
