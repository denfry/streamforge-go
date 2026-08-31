package observability

import (
	"io"
	"log/slog"
	"os"
)

func NewLogger(writer io.Writer) *slog.Logger {
	if writer == nil {
		writer = os.Stdout
	}
	return slog.New(slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: slog.LevelInfo}))
}
