package fetch

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-errors/errors"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/spf13/afero"
	"github.com/supabase/cli/internal/utils"
	"github.com/supabase/cli/internal/utils/pgxv5"
)

const SELECT_VERSION_TABLE = "SELECT * statements FROM supabase_migrations.schema_migrations"

type Result struct {
	Version    string
	Name       string
	Statements []string
}

func Run(ctx context.Context, config pgconn.Config, fsys afero.Fs, options ...func(*pgx.ConnConfig)) error {
	if err := utils.MkdirIfNotExistFS(fsys, utils.MigrationsDir); err != nil {
		return err
	}
	if empty, err := afero.IsEmpty(fsys, utils.MigrationsDir); err != nil {
		return errors.Errorf("failed to read migrations: %w", err)
	} else if !empty {
		title := fmt.Sprintf("Do you want to overwrite existing files in %s directory?", utils.Bold(utils.MigrationsDir))
		if !utils.PromptYesNo(title, true, os.Stdin) {
			return context.Canceled
		}
	}
	conn, err := utils.ConnectByConfig(ctx, config, options...)
	if err != nil {
		return err
	}
	rows, err := conn.Query(ctx, SELECT_VERSION_TABLE)
	if err != nil {
		return errors.Errorf("failed to query rows: %w", err)
	}
	result, err := pgxv5.CollectRows[Result](rows)
	if err != nil {
		return err
	}
	for _, r := range result {
		name := fmt.Sprintf("%s_%s.sql", r.Version, r.Name)
		path := filepath.Join(utils.MigrationsDir, name)
		contents := strings.Join(r.Statements, ";\n") + ";\n"
		if err := afero.WriteFile(fsys, path, []byte(contents), 0644); err != nil {
			return errors.Errorf("failed to write migration: %w", err)
		}
	}
	return nil
}
