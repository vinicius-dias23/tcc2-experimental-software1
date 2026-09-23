// Package metrics expõe contadores simples no formato de texto do Prometheus,
// lidos pelos scripts de coleta em Python (GET /metrics).
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
)

type Registry struct {
	mu       sync.Mutex
	counters map[string]*atomic.Int64
	help     map[string]string
}

func New() *Registry {
	return &Registry{counters: map[string]*atomic.Int64{}, help: map[string]string{}}
}

// Counter devolve (criando se preciso) o contador com o nome dado.
func (r *Registry) Counter(name, help string) *atomic.Int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.counters[name]
	if !ok {
		c = &atomic.Int64{}
		r.counters[name] = c
		r.help[name] = help
	}
	return c
}

func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		r.mu.Lock()
		names := make([]string, 0, len(r.counters))
		for n := range r.counters {
			names = append(names, n)
		}
		r.mu.Unlock()
		sort.Strings(names)
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		for _, n := range names {
			fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", n, r.help[n], n, n, r.counters[n].Load())
		}
	})
}
