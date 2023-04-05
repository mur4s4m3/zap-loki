package zaploki

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type ZapLoki interface {
	Hook(e zapcore.Entry) error
	Sink(u *url.URL) (zap.Sink, error)
	Stop()
	WithCreateLogger(zap.Config) (*zap.Logger, error)
}

type Config struct {
	// Url of the loki server including http:// or https://
	Url          string
	BatchMaxSize int
	BatchMaxWait time.Duration
	// Labels that are added to all log lines,
	// each label becomes a stream
	Labels map[string]string
	// EnableLogLevelLabels adds a label with the log level to each log line,
	// results in a stream for each log level
	// EnableLogLevelLabels bool
	Username string
	Password string
}

type lokiPusher struct {
	config    *Config
	ctx       context.Context
	client    *http.Client
	quit      chan struct{}
	entries   chan logEntry
	waitGroup sync.WaitGroup
	streams   map[string]streamEntries
}

type lokiPushRequest struct {
	Streams []streams `json:"streams"`
}

type streams struct {
	Stream map[string]string `json:"stream"`
	Values [][2]string       `json:"values"`
}

type streamEntries struct {
	label string
	logs  [][2]string
}

type logEntry struct {
	Level        string  `json:"level"`
	ZapTimestamp float64 `json:"ts"`
	Message      string  `json:"msg"`
	Caller       string  `json:"caller"`
	raw          string
	timestamp    time.Time
}

func New(ctx context.Context, cfg Config) ZapLoki {
	c := &http.Client{}

	cfg.Url = strings.TrimSuffix(cfg.Url, "/")
	cfg.Url = fmt.Sprintf("%s/loki/api/v1/push", cfg.Url)

	hook := &lokiPusher{
		config:  &cfg,
		ctx:     ctx,
		client:  c,
		quit:    make(chan struct{}),
		entries: make(chan logEntry),
		streams: make(map[string]streamEntries),
	}

	for id, label := range cfg.Labels {
		hook.streams[id] = streamEntries{
			label: label,
			logs:  [][2]string{},
		}
	}

	// if cfg.EnableLogLevelLabels {
	// 	hook.streams["level"] = ""
	// }

	hook.waitGroup.Add(1)
	go hook.run()
	return hook
}

func (lp *lokiPusher) Hook(e zapcore.Entry) error {
	lp.entries <- logEntry{
		Level:        e.Level.String(),
		ZapTimestamp: float64(e.Time.UnixMilli()),
		Message:      e.Message,
		Caller:       e.Caller.TrimmedPath(),
		timestamp:    time.Now(),
	}
	return nil
}

func (lp *lokiPusher) Sink(u *url.URL) (zap.Sink, error) {
	return newSink(lp), nil
}

func (lp *lokiPusher) Stop() {
	close(lp.quit)
	lp.waitGroup.Wait()
}

func (lp *lokiPusher) WithCreateLogger(cfg zap.Config) (*zap.Logger, error) {
	err := zap.RegisterSink(lokiSinkKey, lp.Sink)
	if err != nil {
		log.Fatal(err)
	}

	fullSinkKey := fmt.Sprintf("%s://", lokiSinkKey)

	if cfg.OutputPaths == nil {
		cfg.OutputPaths = []string{fullSinkKey}
	} else {
		cfg.OutputPaths = append(cfg.OutputPaths, fullSinkKey)
	}

	return cfg.Build()
}

func (lp *lokiPusher) run() {
	var batch []logEntry
	ticker := time.NewTimer(lp.config.BatchMaxWait)

	defer func() {
		if len(batch) > 0 {
			lp.send(batch)
		}

		lp.waitGroup.Done()
	}()

	for {
		select {
		case <-lp.ctx.Done():
			return
		case <-lp.quit:
			return
		case entry := <-lp.entries:
			batch = append(batch, entry)
			if len(batch) >= lp.config.BatchMaxSize {
				lp.send(batch)
				batch = make([]logEntry, 0)
				ticker.Reset(lp.config.BatchMaxWait)
			}
		case <-ticker.C:
			if len(batch) > 0 {
				lp.send(batch)
				batch = make([]logEntry, 0)
			}
			ticker.Reset(lp.config.BatchMaxWait)
		}
	}
}

func (lp *lokiPusher) send(batch []logEntry) error {
	data := lokiPushRequest{}

	for _, entry := range batch {
		v := [2]string{strconv.FormatInt(entry.timestamp.UnixNano(), 10), entry.raw}
		for stream, streamEntries := range lp.streams {
			streamEntries.logs = append(streamEntries.logs, v)
			lp.streams[stream] = streamEntries
		}
	}

	for id, values := range lp.streams {
		s := streams{
			Stream: map[string]string{id: values.label},
			Values: values.logs,
		}
		data.Streams = append(data.Streams, s)
	}

	msg, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal json: %w", err)
	}

	req, err := http.NewRequest("POST", lp.config.Url, bytes.NewBuffer(msg))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if lp.config.Username != "" && lp.config.Password != "" {
		req.SetBasicAuth(lp.config.Username, lp.config.Password)
	}

	resp, err := lp.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	defer resp.Body.Close()

	if 199 < resp.StatusCode && resp.StatusCode < 300 {
		return fmt.Errorf("failed to send request: %s", resp.Status)
	}

	return nil
}
