package main

import (
	"fmt"
	"github.com/devopstaku/voltdbgo/voltdb"
	"time"
)

func main() {
	connPool := voltdb.NewConnectionPool("username", "", "localhost:21212", 5, 30)
	b := connPool.NumConns() + 1
	fmt.Printf("%d\n", b)

	for i := 0; i < 100; i++ {
		poolConn, err := connPool.Acquire()
		fmt.Printf("%#v", poolConn)
		if err != nil {
			panic(err)
		}
		response, _ := poolConn.Call("@AdHoc", "select * from store order by Key limit 3;")
		type Row struct {
			Key   string
			Value string
		}
		var row Row
		for response.Table(0).HasNext() {
			response.Table(0).Next(&row)
			fmt.Printf("Row: %v \n", row)
		}
		time.AfterFunc(3*time.Second, func() { connPool.Release(poolConn) })
		fmt.Printf("poll_size:%d\n", connPool.NumConns())
	}
	connPool.Close()

}
