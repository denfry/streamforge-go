package clickhouse

import (
	"context"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ApplyMigration(ctx context.Context, conn clickhouse.Conn, path string) error {
	sql, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return conn.Exec(ctx, string(sql))
}
