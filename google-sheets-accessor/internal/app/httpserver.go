package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router"
)

type HTTPServer struct {
	logger             *slog.Logger
	applicationService applicationService
	sheetsService      sheetsService
}

func NewHTTPServer(
	applicationService applicationService,
	sheetsService sheetsService,
	logger *slog.Logger,
) *HTTPServer {
	return &HTTPServer{
		logger:             logger,
		applicationService: applicationService,
		sheetsService:      sheetsService,
	}
}

func (s *HTTPServer) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	router.SetupRoutes(
		mux,
		s.applicationService,
		s.sheetsService,
		s.logger,
	)

	secureMux := s.apiVersionMiddleware(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%s", os.Getenv("PORT")),
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
		apiKey := r.Header.Get("X-Api-Version")
		if apiKey != "1" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}
