package main

import (
	"fmt"
	"time"
)

func main() {
	ch := make(chan int, 10)

	go func() {
		time.Sleep(1000 * time.Millisecond)
		ch <- 10
		fmt.Println("push to chan")
	}()

	time.Sleep(100 * time.Millisecond)
	//fmt.Printf("get result from chan: %d\n", <-ch)

	close(ch)
	fmt.Println("close ch")

	time.Sleep(2000 * time.Millisecond)
}
