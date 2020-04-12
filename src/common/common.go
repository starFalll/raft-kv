package common

type Err string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op    string
	Key   string
	Value string
	Err   Err
}
