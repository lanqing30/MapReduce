package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var nOther int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		nOther = nReduce
	case reducePhase:
		ntasks = nReduce
		nOther = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)n", ntasks, phase, nOther)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	//Part III

	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go func(i int) {
			defer wg.Done()
			filename := ""
			if i <= len(mapFiles) {
				filename = mapFiles[i]
			}
			taskArgs := DoTaskArgs{
				JobName:       jobName,
				File:          filename,
				Phase:         phase,
				TaskNumber:    i,
				NumOtherPhase: nOther,
			}

			taskFinished := false

			for taskFinished == false {
				workAddr := <-registerChan
				taskFinished = call(workAddr, "Worker.DoTask", taskArgs, nil)
				go func() { registerChan <- workAddr }()
			}
		}(i)

	}
	wg.Wait()
	fmt.Printf("Schedule: %v donen", phase)
}