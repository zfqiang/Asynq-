package main

import (
	"net/http"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
)

func main() {
	mon := asynqmon.New(asynqmon.Options{
		RedisConnOpt: asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
	})

	mux := http.NewServeMux()

	// 【关键】：
	// 1. Handle 的路径以 "/" 结尾，表示匹配该目录下的所有子路径
	// 2. StripPrefix 的参数不带尾部 "/"，表示要剥离的确切前缀字符串
	mux.Handle("/monitor/", http.StripPrefix("/monitor", mon))

	http.ListenAndServe(":8080", mux)
}
