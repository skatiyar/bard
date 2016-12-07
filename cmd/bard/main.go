package main

import (
	"github.com/SKatiyar/bard"
	"log"
	"strconv"
	"time"
)

func main() {
	if dbErr := bard.Open("db/ole", ":5061", ":5060", 10); dbErr != nil {
		log.Fatalln(dbErr)
	}
	defer func() {
		if closeErr := bard.Close(); closeErr != nil {
			log.Println(closeErr)
		}
	}()

	for i := 0; i < 1; i++ {
		if putErr := bard.Put([]byte(strconv.Itoa(i)), []byte("world")); putErr != nil {
			log.Fatalln(putErr)
		}

		getVal, getValErr := bard.Get([]byte(strconv.Itoa(i)))
		if getValErr != nil {
			log.Fatalln(getValErr)
		}

		log.Println(string(getVal))
	}

	<-time.After(60 * time.Second)
}
