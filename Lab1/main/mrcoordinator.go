package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mr"
import "time"
import "os"
import "fmt"

func main() {
	//命令行参数个数小于2，也就是说没有input file
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}

	//创建一个coordinator，传入input fileNames和Reduce个数
	m := mr.MakeCoordinator(os.Args[1:], 10)

	//检测任务是否结束
	for m.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Second)
}
