package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/imrishabk/goqueue/internal/backoff"
	"github.com/imrishabk/goqueue/internal/database"
	"github.com/imrishabk/goqueue/internal/http/handler"
	"github.com/imrishabk/goqueue/internal/http/middleware"
	"github.com/imrishabk/goqueue/internal/http/routes"
	"github.com/imrishabk/goqueue/internal/store"
	"github.com/joho/godotenv"
)

func init() {
	// .env is optional in Docker where DATABASE_URL is injected
	_ = godotenv.Load()
}

func main() {
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		log.Fatal("DATABASE_URL not set")
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	pool, err := database.NewPool(ctx, connString)
	if err != nil {
		log.Fatal("failed to establish a database pool:", err)
	}
	defer pool.Close()

	st := store.NewPGStoreWithConfig(pool, store.PGConfig{Backoff: backoffPolicyFromEnv()})
	h := handler.NewHandler(st)
	router := routes.NewRouter(h)

	// Liveness sweep: cancellable, every 30s mark workers dead if last_heartbeat < now-45s and requeue their running jobs
	go runSweep(ctx, st)

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = ":8080"
	}
	// ensure addr has colon
	if addr[0] != ':' {
		addr = ":" + addr
	}

	log.Printf("coordinator listening on %s", addr)
	srv := &http.Server{Addr: addr, Handler: middleware.Logger(middleware.APIKey(os.Getenv("API_KEY"), router))}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	<-ctx.Done()
	log.Println("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

// backoffPolicyFromEnv reads BACKOFF_BASE/BACKOFF_CAP durations, falling back to defaults.
func backoffPolicyFromEnv() backoff.Policy {
	p := backoff.Default()
	if s := os.Getenv("BACKOFF_BASE"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			p.Base = d
		}
	}
	if s := os.Getenv("BACKOFF_CAP"); s != "" {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			p.Max = d
		}
	}
	return p
}

func runSweep(ctx context.Context, st store.Store) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deadBefore := time.Now().UTC().Add(-45 * time.Second)
			dead, requeued, err := st.SweepDeadWorkers(ctx, deadBefore)
			if err != nil {
				log.Printf("sweep error: %v", err)
				continue
			}
			if dead > 0 || requeued > 0 {
				log.Printf("sweep: marked %d workers dead, requeued %d jobs", dead, requeued)
			}
		}
	}
}
