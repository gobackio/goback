package retention

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// String is the bracket as <period>=<count>, or the period alone for the
// tail that keeps forever: "daily=60", "monthly".
func (b Bracket) String() string {
	if b.Count == 0 {
		return string(b.Period)
	}

	return fmt.Sprintf("%s=%d", b.Period, b.Count)
}

// ParseBracket reads a bracket as Bracket.String writes it.
func ParseBracket(s string) (Bracket, error) {
	period, count, hasCount := strings.Cut(s, "=")

	b := Bracket{Period: Period(period)}
	if !hasCount {
		return b, nil
	}

	n, err := strconv.Atoi(count)
	if err != nil || n < 1 {
		return Bracket{}, fmt.Errorf("%w: %q is not a number of periods", ErrInvalidPolicy, count)
	}

	b.Count = n

	return b, nil
}

// String is the policy as the flags of goback set that set it:
// "keep-last=1 keep=daily=60 keep=monthly keep-within=24h0m0s".
func (p Policy) String() string {
	var parts []string
	if p.KeepLast > 0 {
		parts = append(parts, fmt.Sprintf("keep-last=%d", p.KeepLast))
	}

	for _, b := range p.Brackets {
		parts = append(parts, "keep="+b.String())
	}

	if p.KeepWithin > 0 {
		within := p.KeepWithin.String()
		if p.KeepWithin == time.Duration(math.MaxInt64) {
			within = "forever"
		}

		parts = append(parts, "keep-within="+within)
	}

	if len(parts) == 0 {
		return "keep nothing"
	}

	return strings.Join(parts, " ")
}
