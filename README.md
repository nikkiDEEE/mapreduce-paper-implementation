# MapReduce Implementation

## Overview

This implementation (in Go) is based on the MapReduce paper developed by Jeffery Dean and Sanjay Ghemawat of Google back in 2004.

## Instructions

In order to run this code on a MapReduce application (e.g. word-count), please navigate to the `mr-main` directory. First, make sure the word-count plugin is freshly built:

```
$ go build -buildmode=plugin ../mr-main/mrapps/wc.go
```

Run the coordinator.

```
$ rm mr-out*
$ go run mrcoordinator.go ../data/pg-*.txt
```

The `../data/pg-*.txt` arguments to `mrcoordinator.go` are the input files; each file corresponds to one "split", and is the input to one Map task.

In one or more **other** windows/terminals, run some workers:

```
$ go run mrworker.go wc.so
```

When the workers and coordinator have finished, look at the output in `mr-out-*`. When you've completed the lab, the sorted union of the output files should match the sequential output, like this:

```
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```
