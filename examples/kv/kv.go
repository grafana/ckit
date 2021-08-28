package main

import (
	"context"
	"fmt"
)

// ErrNotFound is used when a Key isn't found.
type ErrNotFound struct {
	Key string
}

// Error implements error.
func (e ErrNotFound) Error() string {
	return fmt.Sprintf("key %s not found", e.Key)
}

// KV is a Key Value store.
type KV interface {
	Set(ctx context.Context, key, value string) error
	Delete(ctx context.Context, key string) error
	Get(ctx context.Context, key string) (string, error)
}
