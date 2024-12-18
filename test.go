package main

import (
	"github.com/eininst/flog"
	"github.com/google/uuid"
	"github.com/panjf2000/ants/v2"
	"strings"
	"sync"
	"time"
)

var wg sync.WaitGroup

func block() {
	defer wg.Done()
	uid := uuid.NewString()
	uid = strings.Replace(uid, "-", "", -1)
	println(uid)

	time.Sleep(time.Second * 5)

}
func main() {
	flog.SetTimeFormat("2006-01-02 15:04:05,000")
	flog.Info("start")
	flog.Error("www")
	p, _ := ants.NewPool(0)
	defer p.Release()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		_ = p.Submit(block)
	}
	wg.Wait()

	//time.Sleep(1 * time.Second)
	println("end")
}
