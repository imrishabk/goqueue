package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/imrishabk/goqueue/internal/worker"
	"github.com/joho/godotenv"
)

func init() {
	_ = godotenv.Load()
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	hostname, _ := os.Hostname()
	caps := []string{}
	if s := os.Getenv("CAPABILITIES"); s != "" {
		for _, c := range strings.Split(s, ",") {
			if c = strings.TrimSpace(c); c != "" {
				caps = append(caps, c)
			}
		}
	}
	pollInterval := 2 * time.Second
	if s := os.Getenv("POLL_INTERVAL"); s != "" {
		if d, err := time.ParseDuration(s); err == nil {
			pollInterval = d
		}
	}

	reg := worker.NewRegistry()
	reg.Register("", worker.DefaultHandler(""))
	reg.Register("test", worker.Scripted)

	r := worker.NewRunner(worker.Config{
		CoordinatorURL: getenv("COORDINATOR_URL", "http://localhost:8080"),
		WorkerID:       os.Getenv("WORKER_ID"),
		Hostname:       getenv("HOSTNAME", hostname),
		Capabilities:   caps,
		PollInterval:   pollInterval,
		APIKey:         os.Getenv("API_KEY"),
	}, reg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
