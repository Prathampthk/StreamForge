// Package metadata owns bounded control-plane/audit PostgreSQL access.
package metadata

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid metadata connection configuration")
	}
	c.MaxConns = 4
	c.MinConns = 0
	c.ConnConfig.ConnectTimeout = 2 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, c)
	if err != nil {
		return nil, errors.New("initialize metadata pool")
	}
	return &Store{Pool: pool}, nil
}
func FromEnv(ctx context.Context) (*Store, error) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return nil, errors.New("POSTGRES_DSN is required for DLQ/replay audit")
	}
	return Open(ctx, dsn)
}
func (s *Store) Close() { s.Pool.Close() }
