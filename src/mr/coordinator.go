package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type SchedulePhase int

const (
	MapPhase      SchedulePhase = 1
	ReducePhase   SchedulePhase = 2
	CompletePhase SchedulePhase = 3
)

type Task struct {
	fileName  string
	id        int
	startTime time.Time
	status    TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	files      []string
	nReduce    int
	nMap       int // len(files) == nMap
	phase      SchedulePhase
	tasks      []Task
	fTaskCount int

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

type heartbeatMsg struct {
	response *GetTaskReply
	ok       chan struct{}
}

type reportMsg struct {
	request *FinishArgs
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case <-c.doneCh:
		return true
	default:
		return ret
	}
}

func (c *Coordinator) GetTask(request *GetTaskArgs, response *GetTaskReply) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) FinishTask(request *FinishArgs, response *FinishReply) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) schedule() {

	for {
		select {
		case msg := <-c.heartbeatCh:
			c.handleGetTask(&msg)
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			c.finishTask(&msg)
			msg.ok <- struct{}{}
		case <-time.After(time.Second):
			continue
		}
	}
}

func (c *Coordinator) finishTask(report *reportMsg) {
	switch c.phase {
	case MapPhase:
		c.tasks[report.request.TaskNum].status = TaskStatus(report.request.TaskType + 1)
		c.fTaskCount++
		if c.fTaskCount == c.nMap {
			//log.Printf("-------------c.phase update reduce ------------\n")
			c.phase = ReducePhase
			c.fTaskCount = 0
			c.initTask(Reduce, c.nReduce)
		}
	case ReducePhase:
		c.tasks[report.request.TaskNum].status = TaskStatus(report.request.TaskType + 1)
		c.fTaskCount++
		if c.fTaskCount == c.nReduce {
			//log.Printf("-------------c.phase update complete ------------\n")
			c.phase = CompletePhase
			c.doneCh <- struct{}{}
		}
	case CompletePhase:
	default:
		panic("cannot vali phase")
	}
}

func (c *Coordinator) handleGetTask(reply *heartbeatMsg) {
	reply.response.NReduceTasks = c.nReduce
	reply.response.NMapTasks = c.nMap

	switch c.phase {
	case MapPhase:
		//" ----- map phase ----- "
		for i := 0; i < c.nMap; i++ {
			// Assign a task if it's either never been issued, or if it's been to long
			// since it was issue so the worker may have crashed.
			// Note:  if task has never been issued, time is initialized to 0 UTC
			if (c.tasks[i].startTime.IsZero() || c.tasks[i].startTime.Add(10*time.Second).Before(time.Now())) && c.tasks[i].status == Map {
				reply.response.TaskType = Map
				reply.response.TaskNum = i
				reply.response.MapFile = c.files[i]
				c.tasks[i].status = Map
				c.tasks[i].startTime = time.Now()
				c.tasks[i].fileName = c.files[i]
				return
			}
		}

	case ReducePhase:
		//" ----- reduce phase ----- "
		for i := 0; i < c.nReduce; i++ {
			// Assign a task if it's either never been issued, or if it's been to long
			// since it was issue so the worker may have crashed.
			// Note:  if task has never been issued, time is initialized to 0 UTC
			if c.tasks[i].status == Reduce && (c.tasks[i].startTime.IsZero() || c.tasks[i].startTime.Add(10*time.Second).Before(time.Now())) {
				reply.response.TaskType = Reduce
				reply.response.TaskNum = i
				c.tasks[i].status = Reduce
				c.tasks[i].startTime = time.Now()
				return
			}
		}
	case CompletePhase:
	default:
		panic("cannot vali phase")
	}
}

func (c *Coordinator) initMapPhase() {
	c.phase = MapPhase
	c.heartbeatCh = make(chan heartbeatMsg)
	c.reportCh = make(chan reportMsg)
	c.doneCh = make(chan struct{})
	c.initTask(Map, c.nMap)
}

func (c *Coordinator) initTask(typ TaskStatus, n int) {
	c.tasks = make([]Task, n)
	for i := range c.tasks {
		c.tasks[i].status = typ
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.initMapPhase()
	go c.schedule()

	c.server()
	return &c
}
