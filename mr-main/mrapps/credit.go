package main

//
// a MR application you need to develop
// go build -buildmode=plugin credit.go
//

import (
	"cs350/mr"
	"strconv"
	"strings"
)

// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.

// Map returns a kva --> key: agency, value: []userid
func Map(filename string, contents string) []mr.KeyValue {
	var kva []mr.KeyValue
	lines := strings.Split(contents, "\n")
	for i, line := range lines {
		if i == 0 {
			continue // skip first line
		}

		fields := strings.Split(line, ",")
		if len(fields) < 4 {
			continue // skip incomplete data
		}

		year, err := strconv.Atoi(fields[2])
		if err != nil {
			continue // skip lines with invalid year
		}
		creditScore, err := strconv.Atoi(fields[3])
		if err != nil {
			continue // skip lines with invalid credit score
		}

		if year == 2023 && creditScore > 400 {
			agency := fields[1]
			userID := fields[0]
			kva = append(kva, mr.KeyValue{Key: agency, Value: userID})
		}
	}
	return kva
}

// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
func Reduce(key string, values []string) string {
	result := strconv.Itoa(len(values))
	return result
}
