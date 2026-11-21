package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/hibiken/asynq"
)

/*
Asynq 是 Go 语言生态中基于 Redis 开发的分布式异步任务队列框架，专门用于处理后台异步任务，替代传统的 Goroutine 裸写方案。它解决了原生 Goroutine 无法持久化、服务重启任务丢失、没有重试机制、无法监控的痛点，是目前 Go 微服务中异步任务的首选方案。

核心特性：

	支持任务立即执行、延迟执行、定时 Cron 调度
	内置自动重试、指数退避策略，支持自定义重试规则
	多优先级队列，支持 critical/default/low 分级处理
	原生支持分布式多节点水平扩展，保证任务至少执行一次
	自带 CLI 工具和 Web 监控面板，可视化管理所有任务
	支持任务去重、超时控制、死信队列等企业级能力
*/
func main() {

	// 任务类型命名建议用 业务域:动作 格式，避免冲突
	TaskTypeSendEmail := "notification:send_email"
	ctx := context.Background()

	// 1. 初始化 Asynq 客户端
	client := asynq.NewClient(asynq.RedisClientOpt{
		Addr:     "127.0.0.1:6379",
		Password: "", // 有密码必须显式传入，空密码也不能省略
		DB:       0,
	})
	defer client.Close()

	for i := 0; i < 10; i++ {

		queueName := "default"
		if i%2 == 0 {
			queueName = "critical"
		}
		if i > 8 {
			queueName = "low"
		}

		// 2. 构造任务参数（必须序列化为 []byte）
		payload, _ := json.Marshal(map[string]interface{}{
			"to":      fmt.Sprintf("user_%s_%d@example.com", queueName, i),
			"subject": "Asynq 测试邮件",
			"content": "这是一封异步发送的测试邮件",
		})

		// 3. 创建异步任务
		task := asynq.NewTask(
			TaskTypeSendEmail,
			payload,
			// 配置任务选项
			asynq.MaxRetry(3),                                           // 最大重试次数
			asynq.Queue(queueName),                                      // 指定队列
			asynq.ProcessIn(15*time.Second),                             // 延迟执行
			asynq.Timeout(30*time.Second),                               // 单次执行超时时间
			asynq.TaskID(fmt.Sprintf("email_2026_%s_%d", queueName, i)), // 用业务唯一ID作为TaskID
			asynq.Unique(10*time.Second),                                //	// Asynq 原生支持任务去重，通过 Unique 选项可以指定一段时间内相同业务ID的任务不会重复入队：
		)

		//生产环境建议在业务层额外加 Redis 分布式锁做二次幂等校验，彻底避免重复执行问题。

		// 4. 提交任务到队列
		info, err := client.EnqueueContext(ctx, task)
		if err != nil {
			// 捕获重复提交错误
			if errors.Is(err, asynq.ErrTaskIDConflict) {
				log.Fatal("任务重复提交，已忽略", asynq.ErrTaskIDConflict, "任务ID：", info.ID)
			}
			if errors.Is(err, asynq.ErrDuplicateTask) {
				log.Fatal("任务重复提交，已忽略", asynq.ErrDuplicateTask, "任务ID：", info.ID)
			}
			log.Fatal(err)
		}
		println("任务提交成功，任务ID：", info.ID)
	}
}
