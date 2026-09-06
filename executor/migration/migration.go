package main

import (
	"database/sql"
	"log"
	"os"
	"strings"

	"github.com/imrishabk/goqueue/internal/shard"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/pressly/goose/v3"
)

// We force migration to find the .env file as long as migration is ran from project directory
func init() {
	godotenv.Load()
}

func main() {
	cmd, args := "up", os.Args
	if len(args) > 2 {
		cmd = strings.ToLower(args[1])
	}
	for _, dsn := range shard.DSNs() {
		conn, err := sql.Open("pgx", dsn)
		if err != nil {
			log.Fatal(err)
		}
		switch cmd {
		case "up":
			if err = goose.Up(conn, "./migrations"); err != nil {
				log.Fatalf("migrate up %s: %v", maskDSN(dsn), err)
			}
		case "down":
			if err = goose.Down(conn, "./migrations"); err != nil {
				log.Fatalf("migrate down %s: %v", maskDSN(dsn), err)
			}
		default:
			log.Fatal("Invalid command (use up to run the migration or down to remove migrations)")
		}
		_ = conn.Close()
		log.Printf("migrated %s", maskDSN(dsn))
	}
}

// maskDSN hides credentials in logs.
func maskDSN(dsn string) string {
	if i := strings.Index(dsn, "@"); i >= 0 {
		if j := strings.LastIndex(dsn[:i], ":"); j >= 0 {
			return dsn[:j+1] + "***" + dsn[i:]
		}
		return "***" + dsn[i:]
	}
	return dsn
}
