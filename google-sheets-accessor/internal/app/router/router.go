package router

import "net/http"

func SetupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/health-check", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("GSA healthy"))
	})
}
