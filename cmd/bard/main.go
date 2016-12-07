package main

import (
	"github.com/SKatiyar/bard"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	if dbErr := bard.Open(os.Args[1], os.Args[2], 10); dbErr != nil {
		log.Fatalln("1", dbErr)
	}
	defer func() {
		if closeErr := bard.Close(); closeErr != nil {
			log.Println("2", closeErr)
		}
	}()

	i := 0
	for {
		if putErr := bard.Put([]byte(strconv.Itoa(i)), []byte("world")); putErr != nil {
			log.Println("3", putErr)
		}

		getVal, getValErr := bard.Get([]byte(strconv.Itoa(i)))
		if getValErr != nil {
			log.Println("4", getValErr)
		}

		log.Println("5", string(getVal))

		i++
		<-time.After(60 * time.Second)
	}
}
