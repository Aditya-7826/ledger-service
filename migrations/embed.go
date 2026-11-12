// Package migrations embeds the goose SQL migrations so every binary can apply them.
package migrations

import "embed"

// FS holds the SQL migration files.
//
//go:embed *.sql
var FS embed.FS
