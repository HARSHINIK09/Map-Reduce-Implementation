package mapreduce

import (
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

// All ntasks tasks have to be scheduled on workers. Once all tasks
// have completed successfully, schedule() should return.
//
// Your code here (Part III, Part IV).
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var totaljobs, iolength int
	wg := new(sync.WaitGroup)
	if phase == mapPhase {
		totaljobs = len(mapFiles)
		iolength = nReduce
	}
	if phase == reducePhase {
		totaljobs = nReduce
		iolength = len(mapFiles)
	}
	for temp := 0; temp < totaljobs; temp++ {
		wg.Add(1)
		var dotaskargs DoTaskArgs
		if phase == mapPhase {
			dotaskargs = DoTaskArgs{jobName, mapFiles[temp], phase, temp, iolength}
		}
		if phase == reducePhase {
			dotaskargs = DoTaskArgs{jobName, "", phase, temp, iolength}
		}
		go func() {
			for {
				wrkinf := <-registerChan
				status := call(wrkinf, "Worker.DoTask", &dotaskargs, new(struct{}))
				if status == true {
					go func() {
						registerChan <- wrkinf
					}()
					break
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
