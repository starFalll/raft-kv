package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//true - map false -reduce
type MapTaskArgs struct {
	Maptask bool
}

type EndMapArgs struct {
	Tasknumber int
	Files      map[int][]string
}

type EndMapReply struct {
	Err error
}

type ReportReduceArgs struct {
	Tasknumber int
	Filename   string
}

type ReportReduceReply struct {
	Err error
}

//flag 0-err 1-begin map
//2-map end ,need to break loop
type MapReply struct {
	Filename   string
	Tasknumber int
	NReduce    int
	Flag       byte
}

//flag 0-err 1-begin reduce
//2-wait for other map tasks end,sleep for a while,3-all reduce tasks end
type ReduceReply struct {
	Files      []string
	Tasknumber int
	Flag       byte
}
