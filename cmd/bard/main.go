package main

import (
	"github.com/SKatiyar/bard"
	"io/ioutil"
	"log"
	"os"
)

func main() {
	tmpfile, err := ioutil.TempFile("", "bard")
	if err != nil {
		log.Fatal(err)
	}

	defer os.Remove(tmpfile.Name())

	if dbErr := bard.Open(tmpfile.Name(), 3); dbErr != nil {
		log.Fatal(dbErr)
	}

	if putErr := bard.Put([]byte("hello"), []byte("world")); putErr != nil {
		log.Fatal(putErr)
	}

	getVal, getValErr := bard.Get([]byte("hello"))
	if getValErr != nil {
		log.Println(getValErr)
	}

	log.Println(string(getVal))
}
