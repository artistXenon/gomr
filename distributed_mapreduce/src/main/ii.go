package main

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode"

	"github.com/artistxenon/gomr/src/mapreduce"
)

// The mapping function is called once for each piece of the input.
// In this framework, the key is the name of the file that is being processed,
// and the value is the file's contents. The return value should be a slice of
// key/value pairs, each represented by a mapreduce.KeyValue.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
	// TODO: you should complete this to do the inverted index challenge
	tempMap := map[string](map[string]bool){}

	f := func(c rune) bool { return !unicode.IsLetter(c) }
	words := strings.FieldsFunc(value, f)
	for _, word := range words {
		_, ok := tempMap[word]
		if !ok {
			tempMap[word] = map[string]bool{}
		}
		tempMap[word][document] = true
	}

	for k, v := range tempMap {
		keys := make([]string, len(v))
		i := 0
		for k2 := range v {
			keys[i] = k2
			i++
		}
		res = append(res, mapreduce.KeyValue{Key: k, Value: strings.Join(keys, ":")})
	}
	return res
}

// The reduce function is called once for each key generated by Map, with a
// list of that key's string value (merged across all inputs). The return value
// should be a single output value for that key.
func reduceF(key string, values []string) string {
	// TODO: you should complete this to do the inverted index challenge
	files := map[string]bool{}

	for _, value := range values {
		vInLine := strings.Split(value, ":")
		for _, v := range vInLine {
			files[v] = true
		}
	}
	result := make([]string, len(files))
	i := 0
	for k := range files {
		result[i] = k
		i++
	}

	sort.Strings(result)
	return fmt.Sprintf("%d %s", len(result), strings.Join(result, ","))
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}