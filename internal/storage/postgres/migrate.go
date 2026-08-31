package postgres

import (
	"context"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func ApplyMigration(ctx context.Context, pool *pgxpool.Pool, path string) error {
	sql, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, string(sql))
	return err
}
