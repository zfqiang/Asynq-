package main

import (
	"asynq_demo/common"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
)

// Asynq 客户端实例
var client = asynq.NewClient(asynq.RedisClientOpt{Addr: "127.0.0.1:6379"})

// SubmitSendEmailTask 提交发送邮件任务
func SubmitSendEmailTask(ctx context.Context, userID uint, to, subject, content, bizID string, queueName ...string) error {
	if bizID == "" {
		return errors.New("biz_id不能为空")
	}

	traceID, ok := ctx.Value("trace_id").(string)
	if !ok || traceID == "" {
		return errors.New("上下文缺少trace_id")
	}

	queue := "default"
	if len(queueName) > 0 {
		queue = queueName[0]
	}
	switch queue {
	case "critical", "default", "low":
	default:
		return errors.New("无效的优先级队列：可选 critical、default、low")
	}

	// 1. 构造Payload，强制注入链路上下文
	payload := common.SendEmailPayload{
		BasePayload: common.BasePayload{
			TraceID:  traceID, // 从上游上下文传递TraceID
			UserID:   userID,
			BizID:    bizID,
			CreateAt: time.Now().Unix(),
		},
		To:      to,
		Subject: subject,
		Content: content,
	}

	// 2. 序列化Payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	// 3. 构造任务，配置核心参数
	task := asynq.NewTask(
		common.TaskTypeSendEmail,
		payloadBytes,
		// ---------- 幂等性核心配置 ----------
		// 使用业务ID作为TaskID，同一业务ID在队列未执行完成前重复提交会直接报错
		asynq.TaskID(bizID),
		// 队列内去重有效期：10分钟内相同TaskID拒绝重复入队
		asynq.Unique(10*time.Minute),

		// ---------- 重试配置 ----------
		asynq.MaxRetry(5),             // 最大重试5次
		asynq.Timeout(30*time.Second), // 单次执行超时时间30秒

		// ---------- 延迟/定时配置（可选） ----------
		// asynq.ProcessIn(5*time.Second), // 5秒后执行
		// asynq.ProcessAt(time.Now().Add(1*time.Hour)), // 指定时间执行
	)

	// 4. 提交任务到指定优先级队列
	_, err = client.EnqueueContext(ctx, task, asynq.Queue(queue))
	if err != nil {
		// 捕获重复提交错误
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			common.Logger.Warn("任务重复提交，已忽略", zap.String("biz_id", bizID), zap.String("queue", queue))
			return nil
		}
		return err
	}

	common.Logger.Info("任务提交成功", zap.String("biz_id", bizID), zap.String("trace_id", payload.TraceID), zap.String("queue", queue))
	return nil
}

func main() {
	// 模拟HTTP请求上下文，携带TraceID
	ctx := context.WithValue(context.Background(), "trace_id", uuid.NewString())

	// 提交任务：测试幂等，重复提交同一个bizID只会成功一次 "email_20261121_00" + i
	for i := 0; i < 3; i++ {
		bizID := fmt.Sprintf("email_20261121_00%d", i)
		err := SubmitSendEmailTask(
			ctx,
			1001,
			"test@example.com",
			"测试邮件",
			"这是Asynq异步邮件",
			bizID,
			"critical",
		)
		if err != nil {
			common.Logger.Error("提交任务失败", zap.Error(err))
		}
	}
}
