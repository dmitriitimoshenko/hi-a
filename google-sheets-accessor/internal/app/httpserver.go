package app

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/router"
)

type HTTPServer struct{}

func NewHTTPServer() *HTTPServer {
	return &HTTPServer{}
}

func (s *HTTPServer) Run(ctx context.Context) error {
	mux := http.NewServeMux()

	router.SetupRoutes(mux)

	secureMux := apiVersionMiddleware(mux)

	server := &http.Server{
		Addr:    ":8083",
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
		return nil
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
}

func apiVersionMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("X-Api-Version")
		if apiKey != "1" {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}
