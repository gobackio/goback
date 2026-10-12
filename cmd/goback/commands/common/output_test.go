package common

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// capture runs fn in the given mode with stdout and exit captured.
func capture(t *testing.T, jsonOn bool, fn func()) (string, int) {
	t.Helper()

	var out bytes.Buffer
	code := -1

	oldOut, oldExit, oldMode := stdout, exit, jsonMode
	t.Cleanup(func() { stdout, exit = oldOut, oldExit; SetOutput(oldMode) })

	stdout, exit = &out, func(c int) { code = c }
	SetOutput(jsonOn)

	fn()

	return out.String(), code
}

func TestAResultIsJSONOnStdoutOnlyInJSONMode(t *testing.T) {
	type report struct {
		Objects int `json:"objects"`
	}

	printed := false
	out, _ := capture(t, true, func() { Result(report{Objects: 3}, func() { printed = true }) })

	var got report
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, 3, got.Objects)
	require.False(t, printed)

	out, _ = capture(t, false, func() { Result(report{Objects: 3}, func() { printed = true }) })
	require.Empty(t, out)
	require.True(t, printed)
}

func TestAFailureIsAnErrorObjectOnStdoutAndExitsOne(t *testing.T) {
	out, code := capture(t, true, func() { Fatalf("no set %q", "world") })

	var got struct{ Error string }
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, `no set "world"`, got.Error)
	require.Equal(t, 1, code)

	out, code = capture(t, false, func() { Fatal(errors.New("broken")) })
	require.Empty(t, out, "text mode logs the failure instead")
	require.Equal(t, 1, code)
}

func TestAFailureCarriesTheGRPCCodeOfItsError(t *testing.T) {
	refused := fmt.Errorf("listing commits: %w", status.Error(codes.Unauthenticated, "unknown key"))

	for name, fail := range map[string]func(){
		"FailError": func() { FailError(refused) },
		"Fatal":     func() { Fatal(refused) },
		"Fatalf":    func() { Fatalf("store: %v", refused) },
	} {
		out, _ := capture(t, true, fail)

		var got map[string]string
		require.NoError(t, json.Unmarshal([]byte(out), &got), name)
		require.Equal(t, "Unauthenticated", got["code"], name)
	}

	out, _ := capture(t, true, func() { FailError(errors.New("broken")) })

	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Equal(t, "broken", got["error"])
	require.NotContains(t, got, "code")
}
