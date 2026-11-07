// Package metadata owns bounded control-plane/audit PostgreSQL access.
package metadata

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"streamforge/internal/platform"
	"time"
)

type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string) (*Store, error) {
	c, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid metadata connection configuration")
	}
	if ca := os.Getenv("POSTGRES_CA_PEM"); ca != "" {
		if c.ConnConfig.TLSConfig == nil || c.ConnConfig.TLSConfig.InsecureSkipVerify || len(c.ConnConfig.Fallbacks) > 0 {
			return nil, errors.New("PostgreSQL custom CA requires sslmode=verify-full")
		}
		secure, err := platform.TLSConfig(ca)
		if err != nil {
			return nil, err
		}
		c.ConnConfig.TLSConfig = c.ConnConfig.TLSConfig.Clone()
		c.ConnConfig.TLSConfig.RootCAs = secure.RootCAs
		c.ConnConfig.TLSConfig.MinVersion = secure.MinVersion
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
