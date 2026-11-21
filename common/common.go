package common

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// 任务类型常量，全局唯一
const (
	TaskTypeSendEmail    = "task:send_email"    // 发送邮件任务
	TaskTypeProcessOrder = "task:process_order" // 处理订单任务
)

// 通用 Payload 结构，强制携带链路上下文元数据
type BasePayload struct {
	TraceID  string `json:"trace_id"`  // 链路追踪ID，用于日志串联
	UserID   uint   `json:"user_id"`   // 操作人ID
	BizID    string `json:"biz_id"`    // 业务唯一ID（用于幂等，如订单号/邮件ID）
	CreateAt int64  `json:"create_at"` // 任务创建时间
}

// 具体任务 Payload：发送邮件
type SendEmailPayload struct {
	BasePayload
	To      string `json:"to"`      // 收件人
	Subject string `json:"subject"` // 邮件主题
	Content string `json:"content"` // 邮件内容
}

// 错误类型分级，用于重试决策
var (
	// ErrNoRetry 不可重试错误：参数错误、数据不存在、业务校验失败
	ErrNoRetry = errors.New("biz error: no need to retry")
	// ErrRetryable 可重试错误：网络超时、DB临时故障、第三方接口限流
	ErrRetryable = errors.New("temp error: can retry")
)

var (
	RedisClient *redis.Client
	Logger      *zap.Logger
)

// 初始化公共依赖
func init() {
	var err error
	// 初始化日志（生产环境替换为持久化配置）
	Logger, _ = zap.NewProduction()
	defer Logger.Sync()

	// 初始化 Redis 连接
	RedisClient = redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	if err = RedisClient.Ping(context.Background()).Err(); err != nil {
		Logger.Fatal("redis连接失败", zap.Error(err))
	}
}

// 生成幂等Key，用于Redis防重复
func IdempotentKey(bizID string) string {
	return "asynq:idempotent:" + bizID
}
