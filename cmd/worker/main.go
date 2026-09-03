package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
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

func doJSON(client *http.Client, method, url string, body any, out any) (int, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode, nil
}

func main() {
	coordinator := getenv("COORDINATOR_URL", "http://localhost:8080")
	workerID := getenv("WORKER_ID", fmt.Sprintf("worker-%s", uuid.New().String()[:8]))
	hostname, _ := os.Hostname()
	hostname = getenv("HOSTNAME", hostname)
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	client := &http.Client{Timeout: 10 * time.Second}

	// Register (idempotent enough for MVP: coordinator creates; on restart same ID may conflict -> log and continue)
	var registered model.Worker
	code, err := doJSON(client, http.MethodPost, coordinator+"/workers/register",
		map[string]any{"id": workerID, "hostname": hostname, "capabilities": caps}, &registered)
	if err != nil || (code != http.StatusCreated && code != http.StatusOK) {
		log.Printf("register %s -> %d err=%v (continuing, may already exist)", workerID, code, err)
	} else {
		log.Printf("registered worker %s caps=%v", workerID, caps)
	}

	// Heartbeat every 10s per spec
	go func() {
		t := time.NewTicker(10 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				code, err := doJSON(client, http.MethodPost,
					fmt.Sprintf("%s/workers/%s/heartbeat", coordinator, workerID), nil, nil)
				if err != nil || code != http.StatusOK {
					log.Printf("heartbeat -> %d err=%v", code, err)
				}
			}
		}
	}()

	// Poll loop: claim -> execute stub -> complete (fail on panic/error)
	pollURL := fmt.Sprintf("%s/workers/%s/poll", coordinator, workerID)
	for {
		select {
		case <-ctx.Done():
			log.Println("worker shutting down...")
			return
		default:
		}

		var job model.Job
		code, err := doJSON(client, http.MethodPost, pollURL, nil, &job)
		if err != nil {
			log.Printf("poll err=%v, retrying in %s", err, pollInterval)
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}
		if code == http.StatusNoContent {
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}
		if code != http.StatusOK {
			log.Printf("poll -> %d, retrying in %s", code, pollInterval)
			select {
			case <-ctx.Done():
				return
			case <-time.After(pollInterval):
				continue
			}
		}

		log.Printf("claimed job %s type=%s prio=%d", job.ID, job.Type, job.Priority)
		// MVP execute stub: real type->handler registry is v2 per spec Out of Scope
		time.Sleep(100 * time.Millisecond)

		var done model.Job
		code, err = doJSON(client, http.MethodPost,
			fmt.Sprintf("%s/jobs/%s/complete", coordinator, job.ID.String()),
			map[string]any{"worker_id": workerID}, &done)
		if err != nil || code != http.StatusOK {
			if code == http.StatusNotFound {
				log.Printf("complete %s -> 404 job gone (deleted?), moving on", job.ID)
			} else {
				log.Printf("complete %s -> %d err=%v", job.ID, code, err)
			}
			continue
		}
		log.Printf("completed job %s", job.ID)
	}
}
