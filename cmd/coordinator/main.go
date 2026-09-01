package main

import (
	"context"
	"log"
	"net/http"
	"os"

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
	ctx := context.Background()
	pool, err := database.NewPool(ctx, connString)
	if err != nil {
		log.Fatal("failed to establish a database pool:", err)
	}
	defer pool.Close()

	st := store.NewPGStore(pool)
	h := handler.NewHandler(st)
	router := routes.NewRouter(h)

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = ":8080"
	}
	// ensure addr has colon
	if addr[0] != ':' {
		addr = ":" + addr
	}

	log.Printf("coordinator listening on %s", addr)
	if err := http.ListenAndServe(addr, middleware.Logger(router)); err != nil {
		log.Fatal(err)
	}
}
