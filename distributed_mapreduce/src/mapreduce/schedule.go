package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	doneChan := make(chan int)
	switch phase {
	case mapPhase:

		for i, f := range mr.files {
			args := new(DoTaskArgs)
			args.File = f
			args.Phase = phase
			args.JobName = mr.jobName
			args.TaskNumber = i
			args.NumOtherPhase = nios
			go func(i int) {
				w := <-mr.registerChannel
				ok := call(w, "Worker.DoTask", args, new(struct{}))
				if !ok {
					fmt.Printf("some failure from calling map worker\n")
				}
				doneChan <- i
				mr.registerChannel <- w
			}(i)

		}
	case reducePhase:
		for i := 0; i < ntasks; i++ {
			args := new(DoTaskArgs)
			args.File = "" // not used
			args.Phase = phase
			args.JobName = mr.jobName
			args.TaskNumber = i
			args.NumOtherPhase = nios
			go func(i int) {
				w := <-mr.registerChannel
				ok := call(w, "Worker.DoTask", args, new(struct{}))
				if !ok {
					fmt.Printf("some failure from calling reduce worker\n")
				}
				doneChan <- i
				mr.registerChannel <- w
			}(i)
		}
	}

	j := 0
	for i := range doneChan {
		fmt.Printf("task #%d finished\n", i)
		j++
		if j == ntasks {
			return
		}
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	debug("Schedule: %v phase done\n", phase)
}
