package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/dmitriitimoshenko/hi-a/hire-event-processor/internal/app/router"
)

type HTTPServer struct {
	logger *slog.Logger
	config *Config
}

func NewHTTPServer(
	logger *slog.Logger,
	config *Config,
) *HTTPServer {
	return &HTTPServer{
		logger: logger,
		config: config,
	}
}

func (s *HTTPServer) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	router.SetupRoutes(mux)

	secureMux := s.apiVersionMiddleware(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", s.config.Port),
		Handler: secureMux,
	}

	errCh := make(chan error, 1)

	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
			return err
		}

		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}

		return nil
	}

	return nil
}

func (s *HTTPServer) apiVersionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiVersion := r.Header.Get("X-Api-Version")
		if apiVersion != s.config.APIVersion {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]string{
				"error": "Invalid or missing API version",
			})

			return
		}

		next.ServeHTTP(w, r)
	})
}
