package main

import (
	"context"
	"sync"
)

// Store holds key-value data and implements KV.
type Store struct {
	mut  sync.RWMutex
	data map[string]string
}

var _ KV = (*Store)(nil)

// NewStore returns a new Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Set sets the value for a key.
func (s *Store) Set(_ context.Context, key, value string) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	s.data[key] = value
	return nil
}

// Delete removes the value for a key.
func (s *Store) Delete(_ context.Context, key string) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	_, ok := s.data[key]
	delete(s.data, key)
	if !ok {
		return ErrNotFound{Key: key}
	}
	return nil
}

// Get retrieves the value for a key.
func (s *Store) Get(_ context.Context, key string) (val string, err error) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return "", ErrNotFound{Key: key}
	}
	return
}

// Filter will remove any key for which f returns false.
//
// f will not be callled for any keys that have been added
// while Filter is still running.
func (s *Store) Filter(f func(key, value string) bool) {
	type record struct{ Key, Value string }

	s.mut.RLock()
	records := make([]record, 0, len(s.data))
	for k, v := range s.data {
		records = append(records, record{k, v})
	}
	s.mut.RUnlock()

	for _, r := range records {
		if !f(r.Key, r.Value) {
			s.mut.Lock()
			delete(s.data, r.Key)
			s.mut.Unlock()
		}
	}
}
