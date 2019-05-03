package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

type Pair struct {
	First, Second string
}

func tryMutable1(p Pair, first2 string) (p2 Pair) {
	p2 = p
	p2.First=first2
	return
}

func tryMutable2(p *Pair, first2 string) (p2 *Pair) {
	p2 = p
	p2.First=first2
	return
}

func tryMutable3(p *Pair, first2 string) (p2 *Pair){
	*p = Pair{p.First,p.Second}
	p.First = first2
	p2 = p
	return
}

func main() {
	enc := json.NewEncoder(os.Stdout)
	kv := KeyValue{"k1", "v1"}
	err := enc.Encode(kv)
	if err != nil {
		fmt.Println(err)
	}

	err = enc.Encode(Pair{"k2", "v2"})
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println(&Pair{"a", "b"})

	p0 := Pair{"org_first", "org_second"}
	p02 := tryMutable1(p0, "mut2_first")
	fmt.Println(p0)
	fmt.Println(p02)

	p03 := tryMutable2(&p0, "mut3_first")
	fmt.Println(p0)
	fmt.Println(p03)

	pp0 := &Pair{"a1", "a2"}
	tryMutable3(pp0, "a1111")
	fmt.Println(pp0)

	p3 := Pair{"a1", "a2"}
	tryMutable3(&p3, "a1111")
	fmt.Println(p3)
}
