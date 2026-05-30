/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use any a license except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package vfs

import (
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/juicedata/juicefs/pkg/meta"
)

const (
	defaultKafkaChannelSize = 10000
)

// AccessLogRecord 访问日志记录
type AccessLogRecord struct {
	Timestamp   time.Time `json:"timestamp"`
	Method      string    `json:"method"`
	ParentInode uint64    `json:"parent_inode"`
	Name        string    `json:"name"`
	UID         uint32    `json:"uid"`
	GID         uint32    `json:"gid"`
	PID         uint32    `json:"pid"`
	Duration    float64   `json:"duration"` // 耗时（秒）
	Error       uint32    `json:"error"`    // syscall.Errno
}

// KafkaLogger 异步发送访问日志到 Kafka
type KafkaLogger struct {
	producer sarama.AsyncProducer
	topic    string
	channel  chan *AccessLogRecord
	stop     chan struct{}
	wg       sync.WaitGroup

	// 指标
	sent   atomic.Int64
	errors atomic.Int64
}

// NewKafkaLogger 创建一个新的 Kafka Logger
func NewKafkaLogger(brokers string, topic string) (*KafkaLogger, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	config.Producer.RequiredAcks = sarama.NoResponse
	config.Producer.Flush.Frequency = 100 * time.Millisecond
	config.Producer.Flush.Messages = 1000

	producer, err := sarama.NewAsyncProducer(strings.Split(brokers, ","), config)
	if err != nil {
		return nil, err
	}

	k := &KafkaLogger{
		producer: producer,
		topic:    topic,
		channel:  make(chan *AccessLogRecord, defaultKafkaChannelSize),
		stop:     make(chan struct{}),
	}

	k.wg.Add(1)
	go k.run()

	logger.Infof("Kafka logger initialized: brokers=%s, topic=%s", brokers, topic)
	return k, nil
}

// Send 发送一条访问日志记录
func (k *KafkaLogger) Send(record *AccessLogRecord) {
	select {
	case k.channel <- record:
		// 发送到 channel，由后台 goroutine 处理
	default:
		// channel 已满，丢弃这条日志
		k.errors.Add(1)
		logger.Debugf("Kafka logger channel full, dropping log")
	}
}

// run 后台 goroutine，消费 channel 并发送到 Kafka
func (k *KafkaLogger) run() {
	defer k.wg.Done()

	for {
		select {
		case record := <-k.channel:
			k.sendRecord(record)
		case <-k.stop:
			// 处理剩余的日志
			for len(k.channel) > 0 {
				record := <-k.channel
				k.sendRecord(record)
			}
			return
		}
	}
}

// sendRecord 发送单条记录到 Kafka
func (k *KafkaLogger) sendRecord(record *AccessLogRecord) {
	data, err := json.Marshal(record)
	if err != nil {
		k.errors.Add(1)
		logger.Debugf("Failed to marshal access log: %s", err)
		return
	}

	msg := &sarama.ProducerMessage{
		Topic: k.topic,
		Value: sarama.ByteEncoder(data),
	}

	select {
	case k.producer.Input() <- msg:
		k.sent.Add(1)
	case <-time.After(100 * time.Millisecond):
		k.errors.Add(1)
		logger.Debugf("Failed to send to Kafka: timeout")
	}
}

// Close 关闭 Kafka Logger
func (k *KafkaLogger) Close() {
	close(k.stop)
	k.wg.Wait()
	k.producer.Close()

	sent := k.sent.Load()
	errors := k.errors.Load()
	logger.Infof("Kafka logger closed: sent=%d, errors=%d", sent, errors)
}

// GetStats 获取统计信息
func (k *KafkaLogger) GetStats() (int64, int64) {
	return k.sent.Load(), k.errors.Load()
}

// extractInode 从参数中提取 inode
func extractInode(arg interface{}) uint64 {
	switch v := arg.(type) {
	case meta.Ino:
		return uint64(v)
	case uint64:
		return v
	case uint32:
		return uint64(v)
	case int:
		return uint64(v)
	default:
		return 0
	}
}

// extractName 从参数中提取文件名
func extractName(arg interface{}) string {
	switch v := arg.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	case *Entry:
		if v != nil && v.Name != nil {
			return string(v.Name)
		}
	default:
		return ""
	}
	return ""
}

// errnoToUint32 将 syscall.Errno 转换为 uint32
func errnoToUint32(err syscall.Errno) uint32 {
	return uint32(err)
}