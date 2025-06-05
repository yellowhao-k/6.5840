package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type Coordinator struct {
	// Your definitions here.
	tasks             []Task
	reduceTasks       []ReduceTask
	nReduce           int
	mu                sync.Mutex
	nextWorkerID      int
	completedMapTasks int
	totalMapTasks     int
	Phase             NowPhase
}

// Your code here -- RPC handlers for the worker to call.

type TaskStatus int

const (
	NotStarted TaskStatus = iota
	InProgress
	Completed
)

type NowPhase int

const (
	MapPhase NowPhase = iota
	ReducePhase
)

type Task struct {
	TaskID    int
	FileName  string
	Status    TaskStatus
	StartTime time.Time
}
type ReduceTask struct {
	TaskID    int
	Status    TaskStatus
	StartTime time.Time
	NMap      int
}

func (c *Coordinator) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	//分配map任务
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Phase == MapPhase {
		for i, _ := range c.tasks {

			if c.tasks[i].Status == NotStarted {
				c.tasks[i].Status = InProgress

				c.tasks[i].StartTime = time.Now()
				reply.TaskType = TaskTypeMap
				reply.NReduce = c.nReduce
				reply.TaskID = i
				reply.FileName = c.tasks[i].FileName

				return nil
			}
		}
		if c.completedMapTasks < c.totalMapTasks {
			//	这种状况属于任务都分配了，但是还没都完成，申请任务的worker需要等待一下
			reply.TaskType = TaskTypeWait
			return nil
		}
	} else {
		//如果所有map任务都已经被接了，接下来就是reduce
		for i, _ := range c.reduceTasks {
			if c.reduceTasks[i].Status == NotStarted {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskType = TaskTypeReduce
				reply.NMap = len(c.tasks)
				reply.TaskID = i
				return nil
			}
		}
		reply.TaskType = TaskTypeWait
		return nil
	}

	//fmt.Println(c.tasks)
	return nil
}

// map任务完成后 答复
func (c *Coordinator) ReportTaskCompletion(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == TaskTypeMap {
		if c.tasks[args.TaskID].Status == InProgress && c.Phase == MapPhase {

			c.tasks[args.TaskID].Status = Completed
			c.completedMapTasks += 1
			c.tryTransitionPhase()
		}
	} else if args.TaskType == TaskTypeReduce {
		if c.reduceTasks[args.TaskID].Status == InProgress && c.Phase == ReducePhase {
			c.reduceTasks[args.TaskID].Status = Completed
		}
	}
	//	处理Reduce任务的完成情况
	return nil
}

func (c *Coordinator) tryTransitionPhase() {
	//c.mu.Lock() //这里如果不去掉 是否会石锁？ 答案是会死锁 因为不是可重入锁
	//defer c.mu.Unlock()
	if c.Phase == MapPhase && c.completedMapTasks == c.totalMapTasks {
		c.Phase = ReducePhase
		c.initReduceTasks()
	}
}

func (c *Coordinator) initReduceTasks() {

	c.reduceTasks = make([]ReduceTask, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
		log.Printf("开始初始化reduce任务 %+v\n", i)
		c.reduceTasks[i] = ReduceTask{
			TaskID: i,
			NMap:   len(c.tasks),
			Status: NotStarted,
		}

	}

}

// 定时检查任务超时
func (c *Coordinator) monitorTasks() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()
		for i := range c.tasks {
			task := &c.tasks[i]
			if task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second {
				task.Status = NotStarted
				// 可选：记录日志，说明任务被重新分配
			}
		}
		c.mu.Unlock()
	}
}

// 获取workerID
func (c *Coordinator) GetWorkerID(args *GetWorkerIDArgs, reply *GetWorkerIDReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	workerID := c.nextWorkerID
	reply.WorkerID = workerID
	c.nextWorkerID++
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	fmt.Println("我被调用了example")
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
	if c.Phase == ReducePhase {
		for _, task := range c.reduceTasks {
			if task.Status != Completed {
				return ret
			}
		}
		ret = true
	}
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.nextWorkerID = 0

	// Your code here.
	c.tasks = make([]Task, len(files))
	for i, file := range files {
		log.Printf("开始初始化任务 %+v\n", i)
		c.tasks[i] = Task{
			TaskID:   i,
			FileName: file,
			Status:   NotStarted,
		}
	}
	go c.monitorTasks()
	c.totalMapTasks = len(c.tasks)
	c.server()
	return &c
}
