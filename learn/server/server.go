package main

import (
	"asynq_demo/common"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	"go.uber.org/zap"
)

// 任务处理器：处理发送邮件逻辑
func handleSendEmailTask(ctx context.Context, t *asynq.Task) error {
	var payload map[string]interface{}
	// 反序列化任务参数
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		log.Println("参数解析失败，不重试")
		// 参数错误属于永久性错误，返回 SkipRetry 直接跳过重试
		return fmt.Errorf("%w: invalid payload", asynq.SkipRetry)
	}

	// 执行业务逻辑：这里替换为真实的邮件发送代码
	to := payload["to"].(string)
	log.Println("正在发送邮件到：", to)

	// 模拟业务执行
	return nil
}

// 自定义重试延迟函数，添加随机抖动防止惊群效应
var customRetryDelay = func(n int, err error, t *asynq.Task) time.Duration {
	baseDelay := time.Duration(1<<n) * time.Second
	jitter := time.Duration(rand.Int63n(int64(baseDelay) / 5))
	return baseDelay + jitter
}

// Asynq 内置 Scheduler 组件，支持 Cron 表达式定时执行任务：
// 每天凌晨2点执行报表生成任务
func dailyReportTask() {
	scheduler := asynq.NewScheduler(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		&asynq.SchedulerOpts{},
	)

	// 每天凌晨2点执行报表生成任务
	task := asynq.NewTask("report:generate", []byte("{}"))
	// Cron 表达式：秒 分 时 日 月 周
	entryID, err := scheduler.Register("0 0 2 * * *", task)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("scheduled task: %s", entryID)
	// 启动调度器
	if err := scheduler.Run(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	TaskTypeSendEmail := "notification:send_email"

	// 初始化 Asynq 服务端配置
	// Asynq 默认采用指数退避重试策略，重试间隔依次为 1s、2s、4s、8s……最大间隔 30 分钟，默认最大重试 25 次。你可以自定义重试规则： RetryDelayFunc
	server := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		asynq.Config{
			Concurrency: 10, // 并发处理协程数
			Queues: map[string]int{
				"critical": 10, // 高优先级队列
				"default":  5,  // 默认队列
				"low":      1,  // 低优先级队列
			},
			RetryDelayFunc: customRetryDelay,
			// 关闭优雅等待超时，服务关闭时最多等30秒任务完成
			ShutdownTimeout: 30 * time.Second,
		},
	)

	// 启动HTTP服务：暴露监控UI
	go func() {
		// 初始化Asynq Web监控
		mon := asynqmon.New(asynqmon.Options{
			RootPath:     "/mon",
			RedisConnOpt: asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		})
		mux := http.NewServeMux()
		mux.Handle("/mon/", mon)

		common.Logger.Info("监控服务启动在 :8080")
		common.Logger.Info("Web监控地址: http://localhost:8080/mon")
		if err := http.ListenAndServe(":8080", mux); err != nil {
			common.Logger.Fatal("监控服务启动失败", zap.Error(err))
		}
	}()

	// 注册任务路由：任务类型映射到处理函数
	mux := asynq.NewServeMux()
	mux.HandleFunc(TaskTypeSendEmail, handleSendEmailTask)

	// 启动 Worker 服务
	if err := server.Run(mux); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}
