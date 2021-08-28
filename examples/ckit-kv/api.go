package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// API converts API requests into calls to a Store.
type API struct {
	kv KV
}

// NewAPI returns a new API that uses a store for all requests.
func NewAPI(kv KV, r *mux.Router) *API {
	api := &API{kv: kv}

	r.HandleFunc("/api/kv/{key}", api.del).Methods(http.MethodDelete)
	r.HandleFunc("/api/kv/{key}", api.get).Methods(http.MethodGet)
	r.HandleFunc("/api/kv/{key}", api.set).Methods(http.MethodPost, http.MethodPatch)

	return api
}

func (a *API) del(rw http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(rw, "key argument missing", http.StatusBadRequest)
		return
	}

	err := a.kv.Delete(r.Context(), key)
	switch {
	case err == nil:
		rw.WriteHeader(http.StatusOK)
	case errors.As(err, &ErrNotFound{}):
		rw.WriteHeader(http.StatusNotFound)
	case err != nil:
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(err.Error()))
	}
}

func (a *API) get(rw http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(rw, "key argument missing", http.StatusBadRequest)
		return
	}

	value, err := a.kv.Get(r.Context(), key)
	switch {
	case err == nil:
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte(value))
	case errors.As(err, &ErrNotFound{}):
		rw.WriteHeader(http.StatusNotFound)
	case err != nil:
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(err.Error()))
	}
}

func (a *API) set(rw http.ResponseWriter, r *http.Request) {
	key := mux.Vars(r)["key"]
	if key == "" {
		http.Error(rw, "key argument missing", http.StatusBadRequest)
		return
	}

	var sb strings.Builder
	_, err := io.Copy(&sb, r.Body)
	if err != nil {
		http.Error(rw, fmt.Sprintf("failed to read body: %s", err), http.StatusBadRequest)
		return
	}

	err = a.kv.Set(r.Context(), key, sb.String())
	switch {
	case err == nil:
		rw.WriteHeader(http.StatusOK)
	case err != nil:
		rw.WriteHeader(http.StatusInternalServerError)
		rw.Write([]byte(err.Error()))
	}
}
