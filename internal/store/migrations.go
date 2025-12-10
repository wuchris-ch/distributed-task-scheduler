package store

import (
	"context"
	"embed"
	"fmt"
	"strings"
)

//go:embed ../migrations/*.sql
var migrationFiles embed.FS

// RunMigrations executes the embedded SQL migrations in order.
func (s *Store) RunMigrations(ctx context.Context) error {
	entries, err := migrationFiles.ReadDir("../migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		content, err := migrationFiles.ReadFile("../migrations/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		sql := strings.TrimSpace(string(content))
		if sql == "" {
			continue
		}
		if _, err := s.pool.Exec(ctx, sql); err != nil {
			return fmt.Errorf("exec migration %s: %w", e.Name(), err)
		}
	}
	return nil
}
