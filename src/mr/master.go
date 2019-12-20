package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	mu sync.Mutex

	nReduce    int
	leftmap    int
	leftreduce int

	//record unused begin files
	inputfileset  []bool
	receivemapset []bool

	//recored unused mediatefiles
	hashset   map[int]bool
	hashsetbk map[int]bool

	//deal crash
	//Tasknumber: unix time
	mapcrash    map[int]int64
	reducecrash map[int]int64

	//indexinput   []int
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

	//3-all map tasks end,begin to reduce
	if m.leftmap <= 0 {
		reply.Flag = 3
		return nil
	}

	for k, v := range m.inputfileset {

		//1-new map tasks
		if v == false {
			reply.Filename = m.inputfiles[k]
			reply.Tasknumber = k
			reply.NReduce = m.nReduce
			reply.Flag = 1
			m.inputfileset[k] = true
			m.mapcrash[k] = time.Now().Unix()
			return nil
		}

	}

	//2-no not have map task temporarily,wait for a while
	reply.Flag = 2
	return nil

}

func (m *Master) CommitMediateFiles(args *EndMapArgs, reply *EndMapReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//have not received this MediateFile
	log.Printf("tasknumber:%v,m.receivemapset[args.Tasknumber]:%v\n", args.Tasknumber, m.receivemapset[args.Tasknumber])
	if m.receivemapset[args.Tasknumber] == false {
		for k, v := range args.Files {
			m.mediatefiles[k] = append(m.mediatefiles[k], v...)
			m.hashset[k] = true
		}
		m.receivemapset[args.Tasknumber] = true
		m.inputfileset[args.Tasknumber] = true
		m.leftmap--
		//all map tasks end
		log.Printf("CommitMediateFiles map num:%v\n", args.Tasknumber)
		if m.leftmap <= 0 {
			for k, v := range m.hashset {
				m.hashsetbk[k] = v
			}

			m.leftreduce = len(m.hashsetbk)
			log.Printf("CommitMediateFiles end:%v\n", args.Tasknumber)
		}
		return nil

	}
	log.Printf("task num:%v have already received.", args.Tasknumber)
	return nil
}

func (m *Master) AssignReduce(args *MapTaskArgs, reply *ReduceReply) error {
	if args.Maptask == true {
		reply.Flag = 0
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	//3-all reduce tasks end
	if m.leftreduce <= 0 {
		reply.Flag = 3
		return nil
	}

	for k, v := range m.hashset {

		//1-begin reduce
		if v == true {
			reply.Tasknumber = k
			reply.Files = m.mediatefiles[reply.Tasknumber]
			reply.Flag = 1
			m.hashset[k] = false
			m.reducecrash[k] = time.Now().Unix()
			return nil
		}
	}

	//2-no not have reduce task temporarily,wait for a while
	reply.Flag = 2
	return nil

}

func (m *Master) CommitFinalFiles(args *ReportReduceArgs, reply *ReportReduceReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//to prevent re-commit
	if m.hashsetbk[args.Tasknumber] == true {
		m.finalfiles = append(m.finalfiles, args.Filename)
		m.hashsetbk[args.Tasknumber] = false
		m.hashset[args.Tasknumber] = false
		m.leftreduce--
	}

	return nil
}

func (m *Master) EndDealCrash() bool {
	ret := false
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.leftmap > 0 {
		//in map stage
		nowunixtime := time.Now().Unix()
		for k, v := range m.mapcrash {
			if nowunixtime-v > 5 && m.receivemapset[k] == false {
				log.Printf("EndDealCrash() map crash tasknumber:%v,NowUnixTime:%v, LastTime:%v\n",
					k, nowunixtime, v)
				m.inputfileset[k] = false
			}
		}
	} else if m.leftreduce > 0 {
		//in reduce stage
		nowunixtime := time.Now().Unix()
		for k, v := range m.reducecrash {
			if nowunixtime-v > 5 && m.hashsetbk[k] == true {
				log.Printf("EndDealCrash() reduce crash tasknumber:%v,NowUnixTime:%v, LastTime:%v\n",
					k, nowunixtime, v)
				m.hashset[k] = true
			}
		}
	} else {
		ret = true
	}
	return ret
}

func (m *Master) ScheduleDlCrash() {
	time.Sleep(5 * time.Second)
	for m.EndDealCrash() == false {
		time.Sleep(5 * time.Second)
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.hashsetbk) > 0 {
		if m.leftreduce <= 0 {
			log.Printf("(m *Master) Done() end")
			ret = true
		}
	}

	return ret
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.inputfiles = files
	//m.indexinput = make([]int, len(files))

	m.nReduce = nReduce
	m.hashset = make(map[int]bool)
	m.hashsetbk = make(map[int]bool)
	m.mediatefiles = make(map[int][]string)

	m.leftmap = len(files)

	m.inputfileset = make([]bool, len(files))
	m.receivemapset = make([]bool, len(files))

	m.mapcrash = make(map[int]int64)
	m.reducecrash = make(map[int]int64)

	m.server()
	go m.ScheduleDlCrash()

	log.Printf("Init master success!")

	return &m
}
