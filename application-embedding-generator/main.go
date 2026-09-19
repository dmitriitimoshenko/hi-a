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
	"strings"
	"syscall"
	"time"

	"github.com/hire-insight-ai-assistant/application-embedding-generator/internal/app/bus"
)

type config struct {
	SourceTopic      string
	DestinationTopic string
	OpenAIKey        string
	OpenAIModel      string
	OpenAIBaseURL    string
	Port             string
	MaxRetryInterval time.Duration
}

func loadConfig() (config, error) {
	getEnv := func(key string) string {
		return strings.TrimSpace(os.Getenv(key))
	}

	required := map[string]string{
		"STREAM_ADD_APPLICATION_EMBEDDING":  getEnv("STREAM_ADD_APPLICATION_EMBEDDING"),
		"STREAM_SAVE_APPLICATION_EMBEDDING": getEnv("STREAM_SAVE_APPLICATION_EMBEDDING"),
		"OPENAI_API_KEY":                    getEnv("OPENAI_API_KEY"),
	}

	for key, value := range required {
		if value == "" {
			return config{}, errors.New("missing required env: " + key)
		}
	}

	cfg := config{
		SourceTopic:      required["STREAM_ADD_APPLICATION_EMBEDDING"],
		DestinationTopic: required["STREAM_SAVE_APPLICATION_EMBEDDING"],
		OpenAIKey:        required["OPENAI_API_KEY"],
		OpenAIModel:      getEnv("OPENAI_MODEL"),
		OpenAIBaseURL:    getEnv("OPENAI_BASE_URL"),
		Port:             getEnv("PORT"),
		MaxRetryInterval: 10 * time.Second,
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

	return cfg, nil
}

type embeddingGenerator struct {
	cfg        config
	bus        *bus.Client
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

func newEmbeddingGenerator(cfg config, busClient *bus.Client, logger *log.Logger) *embeddingGenerator {
	generator := &embeddingGenerator{
		cfg:        cfg,
		bus:        busClient,
		httpClient: &http.Client{Timeout: 35 * time.Second},
		logger:     logger,
	}

	return generator
}

func (g *embeddingGenerator) run(ctx context.Context) error {
	defer func() {
		if err := g.bus.Close(ctx); err != nil {
			g.logger.Printf("close bus error: %v", err)
		}
	}()

	g.logger.Printf("service started: consuming from %s, producing to %s", g.cfg.SourceTopic, g.cfg.DestinationTopic)

	err := g.bus.Consume(ctx, g.cfg.SourceTopic, g.handle)
	if err != nil {
		return err
	}

	return nil
}

// handle returns nil for permanently malformed entries so they are acknowledged
// and dropped; transient failures return an error and stay pending for retry.
func (g *embeddingGenerator) handle(ctx context.Context, message bus.Message) error {
	appID := strings.TrimSpace(string(message.Key))
	if appID == "" {
		g.logger.Printf("skip message with empty application id")

		return nil
	}

	payload := strings.TrimSpace(string(message.Value))
	if payload == "" {
		g.logger.Printf("skip message with empty payload, application_id=%s", appID)

		return nil
	}

	embedding, err := g.generateEmbedding(ctx, payload)
	if err != nil {
		return fmt.Errorf("embedding failed for application_id=%s: %w", appID, err)
	}

	embeddingBytes, err := json.Marshal(embedding)
	if err != nil {
		g.logger.Printf("failed to marshal embedding for application_id=%s: %v", appID, err)

		return nil
	}

	err = g.bus.Publish(ctx, g.cfg.DestinationTopic, []byte(appID), embeddingBytes)
	if err != nil {
		return fmt.Errorf("failed to publish embedding for application_id=%s: %w", appID, err)
	}

	g.logger.Printf("embedding generated for application_id=%s", appID)

	return nil
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

	busCfg, err := bus.LoadConfig()
	if err != nil {
		log.Fatalf("bus config error: %v", err)
	}

	busClient, err := bus.New(busCfg)
	if err != nil {
		log.Fatalf("bus client error: %v", err)
	}

	generator := newEmbeddingGenerator(cfg, busClient, logger)

	runErr := generator.run(ctx)
	if runErr != nil {
		logger.Printf("service stopped with error: %v", runErr)
	}

	logger.Println("service exited")
}
