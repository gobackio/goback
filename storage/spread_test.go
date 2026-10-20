package storage

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestASpreadClientPutsRequestsOnSeveralConnections(t *testing.T) {
	var (
		mtx   sync.Mutex
		peers = map[string]bool{}
	)

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mtx.Lock()
		peers[r.RemoteAddr] = true
		mtx.Unlock()

		time.Sleep(20 * time.Millisecond)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	base := server.Client().Transport.(*http.Transport).Clone()
	base.ForceAttemptHTTP2 = true
	client := &http.Client{Transport: newSpread(base, 4)}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			response, err := client.Get(server.URL)
			if err == nil {
				_ = response.Body.Close()
			}
		})
	}

	wg.Wait()

	require.Len(t, peers, 4)
}
