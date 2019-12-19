package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu         sync.Mutex
	indexinput int
	nReduce    int
	endmap     bool
	//record unused begin files
	inputfileset map[int]bool

	//recored unused mediatefiles
	hashset   map[int]bool
	hashsetbk map[int]bool

	inputfiles   []string
	mediatefiles map[int][]string
	finalfiles   []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) AssignMap(args *MapTaskArgs, reply *MapReply) error {
	if args.Maptask == false {
		reply.Flag = 0
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.indexinput >= len(m.inputfiles) {
		reply.Flag = 2
		return nil
	}
	reply.Filename = m.inputfiles[m.indexinput]
	reply.Tasknumber = m.indexinput
	reply.NReduce = m.nReduce
	reply.Flag = 1
	m.indexinput++
	return nil

}

func (m *Master) CommitMediateFiles(args *EndMapArgs, reply *EndMapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for k, v := range args.Files {
		m.mediatefiles[k] = append(m.mediatefiles[k], v...)
		m.hashset[k] = true
		m.inputfileset[args.Tasknumber] = true
	}
	//all map tasks end
	log.Printf("CommitMediateFiles map num:%v\n", args.Tasknumber)
	if len(m.inputfileset) == len(m.inputfiles) {
		m.endmap = true
		m.hashsetbk = m.hashset
		log.Printf("CommitMediateFiles end:%v\n", args.Tasknumber)
	}
	return nil
}

func (m *Master) AssignReduce(args *MapTaskArgs, reply *ReduceReply) error {
	if args.Maptask == true {
		reply.Flag = 0
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.endmap == false {
		reply.Flag = 2
		return nil
	}

	for k, v := range m.hashset {
		if v == true {
			reply.Tasknumber = k
			reply.Files = m.mediatefiles[reply.Tasknumber]
			reply.Flag = 1
			m.hashset[k] = false
			return nil
		}
	}

	/**
	if len(m.notusearr) > 0 {
		reply.Tasknumber = m.notusearr[0]
		reply.Files = m.mediatefiles[reply.Tasknumber]
		reply.Flag = 1
		m.notusearr = m.notusearr[1:]
		return nil
	}
	**/
	//no reduce tasks
	reply.Flag = 3
	return nil

}

func (m *Master) CommitFinalFiles(args *ReportReduceArgs, reply *ReportReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.finalfiles = append(m.finalfiles, args.Filename)
	m.hashsetbk[args.Tasknumber] = false
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//os.Remove("mr-socket")
	//l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	time.Sleep(5 * time.Second)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	for _, v := range m.hashsetbk {
		if v == true {
			return ret
		}
	}
	log.Printf("(m *Master) Done() end")
	ret = true
	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.inputfiles = files
	m.indexinput = 0
	m.nReduce = nReduce
	m.hashset = make(map[int]bool)
	m.mediatefiles = make(map[int][]string)
	m.inputfileset = make(map[int]bool)
	m.server()

	return &m
}
