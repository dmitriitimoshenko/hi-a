package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

const kafkaMaxAttempts = 30
const kafkaReaderTimeoutLogInterval = time.Minute

type config struct {
	KafkaBrokers          []string
	KafkaClientID         string
	KafkaConsumerGroup    string
	SourceTopic           string
	DestinationTopic      string
	OpenAIKey             string
	OpenAIModel           string
	OpenAIBaseURL         string
	Port                  string
	MaxRetryInterval      time.Duration
	ProducerBatchSize     int
	ProducerBatchBytes    int
	ProducerBatchTimeout  time.Duration
	ConsumerMinBytes      int
	ConsumerMaxBytes      int
	ConsumerMaxWait       time.Duration
	ShutdownGraceDuration time.Duration
}

func loadConfig() (config, error) {
	getEnv := func(key string) string {
		return strings.TrimSpace(os.Getenv(key))
	}

	required := map[string]string{
		"KAFKA_SERVER":                           getEnv("KAFKA_SERVER"),
		"KAFKA_CLIENT_ID":                        getEnv("KAFKA_CLIENT_ID"),
		"KAFKA_CONSUMER_GROUP":                   getEnv("KAFKA_CONSUMER_GROUP"),
		"KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING":  getEnv("KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING"),
		"KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING": getEnv("KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"),
		"OPENAI_API_KEY":                         getEnv("OPENAI_API_KEY"),
	}

	for key, value := range required {
		if value == "" {
			return config{}, errors.New("missing required env: " + key)
		}
	}

	cfg := config{
		KafkaBrokers:          nil,
		KafkaClientID:         required["KAFKA_CLIENT_ID"],
		KafkaConsumerGroup:    required["KAFKA_CONSUMER_GROUP"],
		SourceTopic:           required["KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING"],
		DestinationTopic:      required["KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING"],
		OpenAIKey:             required["OPENAI_API_KEY"],
		OpenAIModel:           getEnv("OPENAI_MODEL"),
		OpenAIBaseURL:         getEnv("OPENAI_BASE_URL"),
		Port:                  getEnv("PORT"),
		MaxRetryInterval:      10 * time.Second,
		ProducerBatchSize:     1,
		ProducerBatchBytes:    1 << 20,
		ProducerBatchTimeout:  time.Second,
		ConsumerMinBytes:      1,
		ConsumerMaxBytes:      10 * 1024 * 1024,
		ConsumerMaxWait:       500 * time.Millisecond,
		ShutdownGraceDuration: 15 * time.Second,
	}

	brokersRaw := strings.Split(required["KAFKA_SERVER"], ",")
	for _, broker := range brokersRaw {
		trimmed := strings.TrimSpace(broker)
		if trimmed != "" {
			cfg.KafkaBrokers = append(cfg.KafkaBrokers, trimmed)
		}
	}
	if len(cfg.KafkaBrokers) == 0 {
		return config{}, errors.New("no valid Kafka brokers provided")
	}

	if cfg.OpenAIModel == "" {
		cfg.OpenAIModel = "text-embedding-3-small"
	}
	if cfg.OpenAIBaseURL == "" {
		cfg.OpenAIBaseURL = "https://api.openai.com/v1"
	}
	cfg.Port = strings.TrimSpace(cfg.Port)
	if cfg.Port == "" {
		cfg.Port = "8089"
	}

	parsePositiveInt := func(value string) (int, bool) {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed <= 0 {
			return 0, false
		}
		return parsed, true
	}

	if v := getEnv("KAFKA_PRODUCER_BATCH_SIZE"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok {
			cfg.ProducerBatchSize = parsed
		}
	}

	if v := getEnv("KAFKA_PRODUCER_BATCH_BYTES"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok {
			cfg.ProducerBatchBytes = parsed
		}
	}

	if v := getEnv("KAFKA_PRODUCER_BATCH_TIMEOUT_MS"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok {
			cfg.ProducerBatchTimeout = time.Duration(parsed) * time.Millisecond
		}
	}

	if v := getEnv("KAFKA_CONSUMER_MIN_BYTES"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok {
			cfg.ConsumerMinBytes = parsed
		}
	}

	if v := getEnv("KAFKA_CONSUMER_MAX_BYTES"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok && parsed > cfg.ConsumerMinBytes {
			cfg.ConsumerMaxBytes = parsed
		}
	}

	if v := getEnv("KAFKA_CONSUMER_MAX_WAIT_MS"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok {
			cfg.ConsumerMaxWait = time.Duration(parsed) * time.Millisecond
		}
	}

	if v := getEnv("SHUTDOWN_GRACE_SECONDS"); v != "" {
		if parsed, ok := parsePositiveInt(v); ok {
			cfg.ShutdownGraceDuration = time.Duration(parsed) * time.Second
		}
	}

	return cfg, nil
}

type embeddingGenerator struct {
	cfg        config
	reader     *kafka.Reader
	writer     *kafka.Writer
	httpClient *http.Client
	logger     *log.Logger
}

type embeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

type permanentError struct {
	StatusCode int
	Body       string
}

func (e *permanentError) Error() string {
	return fmt.Sprintf("openai returned status %d: %s", e.StatusCode, e.Body)
}

type throttledKafkaReaderLogger struct {
	logger             *log.Logger
	mutex              sync.Mutex
	lastTimeoutLogAt   time.Time
	suppressedTimeouts int
}

func (l *throttledKafkaReaderLogger) log(msg string, args ...interface{}) {
	formattedMessage := fmt.Sprintf(msg, args...)
	if !l.isTransientKafkaReaderTimeout(formattedMessage) {
		l.logger.Printf("kafka reader error: %s", formattedMessage)

		return
	}

	l.mutex.Lock()
	defer l.mutex.Unlock()

	now := time.Now()
	if l.lastTimeoutLogAt.IsZero() || now.Sub(l.lastTimeoutLogAt) >= kafkaReaderTimeoutLogInterval {
		if l.suppressedTimeouts > 0 {
			formattedMessage = fmt.Sprintf("%s (suppressed %d similar timeout logs)", formattedMessage, l.suppressedTimeouts)
		}

		l.lastTimeoutLogAt = now
		l.suppressedTimeouts = 0

		l.logger.Printf("kafka reader error: %s", formattedMessage)

		return
	}

	l.suppressedTimeouts++
}

func (l *throttledKafkaReaderLogger) isTransientKafkaReaderTimeout(msg string) bool {
	return strings.Contains(msg, "unknown error reading partition") && strings.Contains(msg, "i/o timeout")
}

func newEmbeddingGenerator(cfg config, logger *log.Logger) *embeddingGenerator {
	readerLogger := &throttledKafkaReaderLogger{logger: logger}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cfg.KafkaBrokers,
		GroupID:     cfg.KafkaConsumerGroup,
		Topic:       cfg.SourceTopic,
		MinBytes:    cfg.ConsumerMinBytes,
		MaxBytes:    cfg.ConsumerMaxBytes,
		MaxWait:     cfg.ConsumerMaxWait,
		MaxAttempts: kafkaMaxAttempts,
		StartOffset: kafka.FirstOffset,
		Logger:      kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(readerLogger.log),
	})

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.KafkaBrokers...),
		Topic:                  cfg.DestinationTopic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: false,
		BatchSize:              cfg.ProducerBatchSize,
		BatchBytes:             int64(cfg.ProducerBatchBytes),
		BatchTimeout:           cfg.ProducerBatchTimeout,
		MaxAttempts:            kafkaMaxAttempts,
		Logger:                 kafka.LoggerFunc(func(msg string, args ...interface{}) {}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Printf("kafka writer error: "+msg, args...)
		}),
	}

	return &embeddingGenerator{
		cfg:        cfg,
		reader:     reader,
		writer:     writer,
		httpClient: &http.Client{Timeout: 35 * time.Second},
		logger:     logger,
	}
}

func (g *embeddingGenerator) run(ctx context.Context) error {
	defer func() {
		if err := g.reader.Close(); err != nil {
			g.logger.Printf("close reader error: %v", err)
		}
		if err := g.writer.Close(); err != nil {
			g.logger.Printf("close writer error: %v", err)
		}
	}()

	g.logger.Printf("service started: consuming from %s, producing to %s", g.cfg.SourceTopic, g.cfg.DestinationTopic)

	for {
		msg, err := g.reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			g.logger.Printf("read message error: %v", err)
			continue
		}

		appID := strings.TrimSpace(string(msg.Key))
		if appID == "" {
			g.logger.Printf("skip message with empty application id, offset=%d", msg.Offset)
			continue
		}

		payload := strings.TrimSpace(string(msg.Value))
		if payload == "" {
			g.logger.Printf("skip message with empty payload, application_id=%s offset=%d", appID, msg.Offset)
			continue
		}

		embedding, err := g.generateEmbedding(ctx, payload)
		if err != nil {
			g.logger.Printf("embedding failed for application_id=%s: %v", appID, err)
			continue
		}

		embeddingBytes, err := json.Marshal(embedding)
		if err != nil {
			g.logger.Printf("failed to marshal embedding for application_id=%s: %v", appID, err)
			continue
		}

		err = g.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(appID),
			Value: embeddingBytes,
		})
		if err != nil {
			g.logger.Printf("failed to publish embedding for application_id=%s: %v", appID, err)
			continue
		}

		g.logger.Printf("embedding generated for application_id=%s (offset=%d)", appID, msg.Offset)
	}
}

func (g *embeddingGenerator) generateEmbedding(ctx context.Context, content string) ([]float64, error) {
	request := embeddingRequest{
		Model: g.cfg.OpenAIModel,
		Input: []string{content},
	}

	payload, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	var result []float64

	operation := func() error {
		requestCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		url := strings.TrimRight(g.cfg.OpenAIBaseURL, "/") + "/embeddings"
		req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return err
		}

		req.Header.Set("Authorization", "Bearer "+g.cfg.OpenAIKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := g.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 {
			if _, copyErr := io.Copy(io.Discard, resp.Body); copyErr != nil {
				return copyErr
			}
			return fmt.Errorf("openai temporary error: %s", resp.Status)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
			return &permanentError{
				StatusCode: resp.StatusCode,
				Body:       string(body),
			}
		}

		var embeddingResp embeddingResponse
		if err := json.NewDecoder(resp.Body).Decode(&embeddingResp); err != nil {
			return err
		}

		if len(embeddingResp.Data) == 0 || len(embeddingResp.Data[0].Embedding) == 0 {
			return errors.New("empty embedding data")
		}

		result = embeddingResp.Data[0].Embedding

		return nil
	}

	err = retryWithBackoff(ctx, 500*time.Millisecond, g.cfg.MaxRetryInterval, 6, operation)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func retryWithBackoff(
	ctx context.Context,
	initialDelay time.Duration,
	maxDelay time.Duration,
	maxAttempts int,
	operation func() error,
) error {
	delay := initialDelay

	if delay <= 0 {
		delay = 250 * time.Millisecond
	}
	if maxDelay <= 0 {
		maxDelay = 5 * time.Second
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := operation()
		if err == nil {
			return nil
		}

		var permErr *permanentError
		if errors.As(err, &permErr) {
			return err
		}

		if attempt == maxAttempts {
			return err
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}

		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return nil
}

func startHealthServer(ctx context.Context, port string, logger *log.Logger) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health-check", func(w http.ResponseWriter, r *http.Request) {
		if !acceptsAPIVersion(r) {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}

		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		response := healthResponse{
			Status: "ok",
		}

		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			logger.Printf("health response encode error: %v", err)
		}
	})

	addr := strings.TrimSpace(port)
	if addr == "" {
		addr = "8089"
	}
	if !strings.Contains(addr, ":") {
		addr = ":" + addr
	}

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("health server shutdown error: %v", err)
		}
	}()

	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Printf("health server error: %v", err)
		}
	}()
}

type healthResponse struct {
	Status string `json:"status"`
}

func acceptsAPIVersion(r *http.Request) bool {
	version := r.Header.Get("x-api-version")
	if version == "" {
		version = r.Header.Get("X-API-Version")
	}

	if version == "" {
		return false
	}

	expected := strings.TrimSpace(os.Getenv("API_VERSION"))
	if expected == "" {
		expected = "1"
	}

	return version == expected
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := log.New(os.Stdout, "[embedding-generator] ", log.LstdFlags|log.LUTC|log.Lmsgprefix)

	startHealthServer(ctx, cfg.Port, logger)

	generator := newEmbeddingGenerator(cfg, logger)

	runErr := generator.run(ctx)
	if runErr != nil {
		logger.Printf("service stopped with error: %v", runErr)
	}

	logger.Println("service exited")
}
