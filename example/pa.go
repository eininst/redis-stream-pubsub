package main

import (
	"github.com/eininst/flog"
	"runtime"
)

func main() {
	poolSize := 10 * runtime.GOMAXPROCS(0)
	minPoolSize := poolSize / 5

	flog.Info(poolSize)
	flog.Info(minPoolSize)
}
