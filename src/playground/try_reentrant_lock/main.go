package main

import (
	"fmt"
	"sync"
)

func main() {
	mu := sync.Mutex{}

	mu.Lock()
	fmt.Println("lock 1")
	mu.Lock()
	fmt.Println("lock 2")
	mu.Unlock()
	fmt.Println("unlock 1")
	mu.Unlock()
	fmt.Println("unlock 2")
}

