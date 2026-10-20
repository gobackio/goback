package storage

import (
	"net/http"
	"sync/atomic"
)

// locationConns is how many connections a client spreads its requests to
// locations over: one HTTP/2 connection to a bucket carries about as much
// as one request does, however many share it.
const locationConns = 16

// spread hands requests to its transports in turn, each keeping its own
// connections.
type spread struct {
	transports []http.RoundTripper
	next       atomic.Uint64
}

func newSpread(base *http.Transport, n int) *spread {
	s := &spread{transports: make([]http.RoundTripper, n)}
	for i := range s.transports {
		s.transports[i] = base.Clone()
	}

	return s
}

func (s *spread) RoundTrip(r *http.Request) (*http.Response, error) {
	return s.transports[s.next.Add(1)%uint64(len(s.transports))].RoundTrip(r)
}
