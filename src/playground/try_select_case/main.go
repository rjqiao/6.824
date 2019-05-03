package main

import (
	"fmt"
	"time"
)

func main() {
	done := make(chan int, 1)
	go func() {
		time.Sleep(time.Millisecond * 3000)
		close(done)
	}()

	select {
	case x := <-done:
		fmt.Println("inside done")
		fmt.Println(x)
	}
	fmt.Println("finish")
}
