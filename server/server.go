package main

import (
	"asynq_demo/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/hibiken/asynq"
	"github.com/hibiken/asynqmon"
	"go.uber.org/zap"
)

// 自定义指数退避重试策略（带随机抖动防止惊群）
func retryDelayFunc(n int, err error, t *asynq.Task) time.Duration {
	// 重试间隔：1s, 2s, 4s, 8s, 16s，加±20%随机抖动
	baseDelay := time.Duration(1<<n) * time.Second
	jitter := time.Duration(rand.Int63n(int64(baseDelay) / 5))
	if rand.Intn(2) == 0 {
		return baseDelay + jitter
	}
	return baseDelay - jitter
}

// 自定义重试决策：仅对可重试错误执行重试
func retryFunc(err error) bool {
	// 业务错误直接标记失败，不重试
	if errors.Is(err, common.ErrNoRetry) {
		return false
	}
	// 临时错误允许重试
	return true
}

// 邮件任务处理器
func handleSendEmailTask(ctx context.Context, t *asynq.Task) error {
	// ---------- 步骤1：解析Payload，注入上下文 ----------
	var payload common.SendEmailPayload
	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		common.Logger.Error("Payload解析失败，参数错误不重试", zap.Error(err))
		return fmt.Errorf("%w: %v", common.ErrNoRetry, err)
	}
	// 将链路元数据注入上下文，后续日志、DB调用都携带这些信息
	ctx = context.WithValue(ctx, "trace_id", payload.TraceID)
	ctx = context.WithValue(ctx, "user_id", payload.UserID)
	ctx = context.WithValue(ctx, "biz_id", payload.BizID)

	log := common.Logger.With(
		zap.String("trace_id", payload.TraceID),
		zap.String("biz_id", payload.BizID),
		zap.String("task_type", t.Type()),
	)
	log.Info("开始处理任务")

	// ---------- 步骤2：幂等性二次校验（防止队列穿透重复执行） ----------
	// 即使客户端去重失效，这里再次通过Redis锁保证同一业务ID只会执行一次
	lockKey := common.IdempotentKey(payload.BizID)
	set, err := common.RedisClient.SetNX(ctx, lockKey, "processing", 24*time.Hour).Result()
	if err != nil {
		log.Error("幂等锁获取失败，可重试", zap.Error(err))
		return fmt.Errorf("%w: redis lock failed", common.ErrRetryable)
	}
	if !set {
		log.Warn("任务已处理/处理中，跳过重复执行")
		return nil
	}

	// ---------- 步骤3：执行业务逻辑，分级处理错误 ----------
	log.Info("开始发送邮件", zap.String("to", payload.To))
	// 模拟业务错误场景
	if payload.To == "" {
		log.Error("收件人地址为空，参数错误不重试")
		common.RedisClient.Del(ctx, lockKey) // 参数错误主动释放锁，允许修正后重试
		return fmt.Errorf("%w: empty email address", common.ErrNoRetry)
	}

	// 模拟临时网络错误（30%概率失败，触发重试）
	if rand.Float32() < 0.3 {
		log.Warn("邮件服务网络超时，将重试")
		// 不删除幂等锁，防止重试期间重复提交
		return fmt.Errorf("%w: email service timeout", common.ErrRetryable)
	}

	// 模拟业务执行成功
	time.Sleep(2 * time.Second)
	log.Info("邮件发送成功")

	// ---------- 步骤4：处理完成后更新幂等锁状态（可选） ----------
	// 标记任务完成，24小时内相同业务ID拒绝执行
	common.RedisClient.Set(ctx, lockKey, "success", 24*time.Hour)
	return nil
}

func main() {
	// 初始化 Asynq 服务器
	srv := asynq.NewServer(
		asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		asynq.Config{
			Concurrency: 10, // 并发处理协程数
			Queues: map[string]int{
				"critical": 10, // 高优先级队列
				"default":  5,  // 默认队列
				"low":      1,  // 低优先级队列
			},
			// 全局重试配置
			RetryDelayFunc: retryDelayFunc,
			IsFailure:      retryFunc,
			// 关闭优雅等待超时，服务关闭时最多等30秒任务完成
			ShutdownTimeout: 30 * time.Second,
		},
	)

	// 注册任务处理器
	mux := asynq.NewServeMux()
	mux.HandleFunc(common.TaskTypeSendEmail, handleSendEmailTask)
	// 可继续注册其他任务处理器...
	// mux.HandleFunc(TaskTypeProcessOrder, handleProcessOrderTask)

	// 启动HTTP服务：暴露监控UI
	go func() {
		// 初始化Asynq Web监控
		mon := asynqmon.New(asynqmon.Options{
			RootPath:     "/mon",
			RedisConnOpt: asynq.RedisClientOpt{Addr: "127.0.0.1:6379"},
		})
		mux := http.NewServeMux()
		mux.Handle("/mon/", mon)

		common.Logger.Info("监控服务启动在 :9090")
		common.Logger.Info("Web监控地址: http://localhost:9090/mon")
		if err := http.ListenAndServe(":9090", mux); err != nil {
			common.Logger.Fatal("监控服务启动失败", zap.Error(err))
		}
	}()

	// 启动任务处理服务
	common.Logger.Info("Asynq任务服务启动")
	if err := srv.Run(mux); err != nil {
		common.Logger.Fatal("任务服务启动失败", zap.Error(err))
	}
}
