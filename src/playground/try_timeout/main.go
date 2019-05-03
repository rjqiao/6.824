package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const times = 100
const timeout_total = 1000 * time.Millisecond
const timeout_each = 200 * time.Millisecond
const time_sleep_base = 150
const time_sleep_rand = 100

var ch_count = make(chan int, times)
var ch_count_in_timeout = make(chan int, times)
var ch_finish = make(chan int, 1)

type Args struct {
}

type Reply struct {
}

func rpc_call_1(args *Args, reply *Reply) (ok bool) {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(time_sleep_rand)+time_sleep_base))
	ch_count <- 1
	return true
}

func rpc_call_1_timeout(args *Args, reply *Reply, timeout time.Duration) (ok bool) {
	ch := make(chan bool, 1)
	exit := make(chan int, 1)
	go func() {
		ch <- rpc_call_1(args, reply)

		select {
		case <-exit:
			return
		default:
		}

		// some else code after rpc within timeout
		ch_count_in_timeout <- 1
	}()

	select {
	case ok = <-ch:
		return ok
	case <-time.After(timeout):
		// 丢弃结果
		exit <- 1
		return false
	}
}

func print_counts() {
	count := 0
	count_in_timeout := 0
	for {
		select {
		case <-ch_count_in_timeout:
			count_in_timeout++
			fmt.Printf("rpc in timeout %d\n", count_in_timeout)
		case <-ch_count:
			count++
			fmt.Printf("rpc %d\n", count)
		case <-ch_finish:
			fmt.Printf("finish count\n")
			return
		}
	}
}

// make 10 rpc call, timeout
func f1() {

	oks := make([]bool, times, times)
	for i := range oks {
		oks[i] = false
	}

	t1 := time.Now()
	go print_counts()
	wg := sync.WaitGroup{}
	for i := range oks {
		wg.Add(1)
		go func(i0 int) {
			oks[i0] = rpc_call_1_timeout(&Args{}, &Reply{}, timeout_each)
			wg.Done()
		}(i)
	}
	wg.Wait()
	ch_finish <- 1

	c :=0
	for _,x := range oks {
		if x==true{
			c++
		}
	}
	fmt.Printf("good result: %d\n", c)

	t2 := time.Now()
	diff := t2.Sub(t1)
	fmt.Println(diff)
}

func main() {
	f1()
}
