package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/imrishabk/goqueue/internal/model"
)

// Config tunes a Runner. Zero PollInterval/HeartbeatInterval get defaults.
type Config struct {
	CoordinatorURL    string
	WorkerID          string
	Hostname          string
	Capabilities      []string
	PollInterval      time.Duration
	HeartbeatInterval time.Duration
}

func (c *Config) withDefaults() {
	if c.PollInterval <= 0 {
		c.PollInterval = 2 * time.Second
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 10 * time.Second
	}
	if c.WorkerID == "" {
		c.WorkerID = fmt.Sprintf("worker-%s", uuid.New().String()[:8])
	}
	if c.Capabilities == nil {
		c.Capabilities = []string{}
	}
}

// Runner polls the coordinator, executes jobs through a Registry, and
// reports complete/fail. Create with NewRunner.
type Runner struct {
	cfg    Config
	reg    *Registry
	client *http.Client
	log    *log.Logger
}

// NewRunner builds a Runner. A nil registry executes nothing successfully:
// register handlers (or a "" default) before Run.
func NewRunner(cfg Config, reg *Registry) *Runner {
	cfg.withDefaults()
	if reg == nil {
		reg = NewRegistry()
	}
	return &Runner{cfg: cfg, reg: reg, client: &http.Client{Timeout: 10 * time.Second}, log: log.Default()}
}

func (r *Runner) doJSON(method, url string, body any, out any) (int, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode != http.StatusNoContent && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			return resp.StatusCode, err
		}
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode, nil
}

// sleepOrDone waits d unless ctx ends first.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Run registers, heartbeats, and polls until ctx ends. It returns nil on
// clean shutdown; startup register failure is logged, not fatal (the ID
// may already exist from a previous run).
func (r *Runner) Run(ctx context.Context) error {
	base := r.cfg.CoordinatorURL
	id := r.cfg.WorkerID

	var registered model.Worker
	code, err := r.doJSON(http.MethodPost, base+"/workers/register",
		map[string]any{"id": id, "hostname": r.cfg.Hostname, "capabilities": r.cfg.Capabilities}, &registered)
	if err != nil || (code != http.StatusCreated && code != http.StatusOK) {
		r.log.Printf("register %s -> %d err=%v (continuing, may already exist)", id, code, err)
	} else {
		r.log.Printf("registered worker %s caps=%v", id, r.cfg.Capabilities)
	}

	go func() {
		t := time.NewTicker(r.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				code, err := r.doJSON(http.MethodPost,
					fmt.Sprintf("%s/workers/%s/heartbeat", base, id), nil, nil)
				if err != nil || code != http.StatusOK {
					r.log.Printf("heartbeat -> %d err=%v", code, err)
				}
			}
		}
	}()

	pollURL := fmt.Sprintf("%s/workers/%s/poll", base, id)
	for {
		select {
		case <-ctx.Done():
			r.log.Println("worker shutting down...")
			return nil
		default:
		}

		var job model.Job
		code, err := r.doJSON(http.MethodPost, pollURL, nil, &job)
		if err != nil || (code != http.StatusOK && code != http.StatusNoContent) {
			r.log.Printf("poll -> %d err=%v, retrying in %s", code, err, r.cfg.PollInterval)
			if !sleepOrDone(ctx, r.cfg.PollInterval) {
				return nil
			}
			continue
		}
		if code == http.StatusNoContent {
			if !sleepOrDone(ctx, r.cfg.PollInterval) {
				return nil
			}
			continue
		}

		r.log.Printf("claimed job %s type=%s prio=%d", job.ID, job.Type, job.Priority)
		if herr := r.reg.Execute(ctx, job.Type, job.Payload); herr != nil {
			r.failJob(job, herr.Error())
			continue
		}
		r.completeJob(job)
	}
}

func (r *Runner) completeJob(job model.Job) {
	var done model.Job
	code, err := r.doJSON(http.MethodPost,
		fmt.Sprintf("%s/jobs/%s/complete", r.cfg.CoordinatorURL, job.ID.String()),
		map[string]any{"worker_id": r.cfg.WorkerID}, &done)
	switch {
	case code == http.StatusNotFound:
		r.log.Printf("complete %s -> 404 job gone (deleted?), moving on", job.ID)
	case err != nil || code != http.StatusOK:
		r.log.Printf("complete %s -> %d err=%v", job.ID, code, err)
	default:
		r.log.Printf("completed job %s", job.ID)
	}
}

func (r *Runner) failJob(job model.Job, msg string) {
	var failed model.Job
	code, err := r.doJSON(http.MethodPost,
		fmt.Sprintf("%s/jobs/%s/fail", r.cfg.CoordinatorURL, job.ID.String()),
		map[string]any{"worker_id": r.cfg.WorkerID, "error": msg}, &failed)
	switch {
	case code == http.StatusNotFound:
		r.log.Printf("fail %s -> 404 job gone (deleted?), moving on", job.ID)
	case err != nil || code != http.StatusOK:
		r.log.Printf("fail %s -> %d err=%v", job.ID, code, err)
	default:
		r.log.Printf("failed job %s: %s", job.ID, msg)
	}
}
