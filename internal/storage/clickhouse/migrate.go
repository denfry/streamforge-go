package clickhouse

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func ApplyMigration(ctx context.Context, conn clickhouse.Conn, path string) error {
	sql, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for _, statement := range strings.Split(string(sql), ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if err := conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("execute ClickHouse migration statement: %w", err)
		}
	}
	return nil
}
