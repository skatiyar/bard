package main

import (
	"github.com/SKatiyar/bard"
	"log"
)

func main() {
	if dbErr := bard.Open("db/ole", 12); dbErr != nil {
		log.Fatal(dbErr)
	}
	defer func() {
		if closeErr := bard.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()

	if putErr := bard.Put([]byte("hello"), []byte("world")); putErr != nil {
		log.Fatal(putErr)
	}

	getVal, getValErr := bard.Get([]byte("hello"))
	if getValErr != nil {
		log.Println(getValErr)
	}

	log.Println(string(getVal))
}
