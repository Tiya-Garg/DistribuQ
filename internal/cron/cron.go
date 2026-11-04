package cron

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a parsed 5-field cron schedule: minute, hour, dom, month, dow.
type Schedule struct {
	expr string
}

// Parse parses a standard 5-field cron expression (minute hour day-of-month month day-of-week).
// Supports standard numbers, asterisks (*), and step values (*/n).
func Parse(expr string) (*Schedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != 5 {
		return nil, errors.New("cron: expression must contain exactly 5 space-separated fields")
	}

	// Validate fields
	if _, err := parseField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("cron minute: %w", err)
	}
	if _, err := parseField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("cron hour: %w", err)
	}
	if _, err := parseField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("cron day of month: %w", err)
	}
	if _, err := parseField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("cron month: %w", err)
	}
	if _, err := parseField(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("cron day of week: %w", err)
	}

	return &Schedule{expr: strings.Join(fields, " ")}, nil
}

func parseField(f string, minVal, maxVal int) (map[int]bool, error) {
	allowed := make(map[int]bool)
	if f == "*" {
		for i := minVal; i <= maxVal; i++ {
			allowed[i] = true
		}
		return allowed, nil
	}

	if strings.HasPrefix(f, "*/") {
		step, err := strconv.Atoi(f[2:])
		if err != nil || step <= 0 {
			return nil, errors.New("invalid step value")
		}
		for i := minVal; i <= maxVal; i += step {
			allowed[i] = true
		}
		return allowed, nil
	}

	val, err := strconv.Atoi(f)
	if err != nil || val < minVal || val > maxVal {
		return nil, fmt.Errorf("value %q out of range [%d, %d]", f, minVal, maxVal)
	}
	allowed[val] = true
	return allowed, nil
}

// Next returns the next scheduled time strictly after t (truncated to minutes).
func (s *Schedule) Next(t time.Time) time.Time {
	fields := strings.Fields(s.expr)
	minMap, _ := parseField(fields[0], 0, 59)
	hourMap, _ := parseField(fields[1], 0, 23)
	domMap, _ := parseField(fields[2], 1, 31)
	monthMap, _ := parseField(fields[3], 1, 12)
	dowMap, _ := parseField(fields[4], 0, 6)

	// Advance to start of next minute
	cur := t.Truncate(time.Minute).Add(time.Minute)

	// Look ahead up to 5 years (minute-by-minute with smart hour/day jumps)
	for i := 0; i < 5*365*24*60; i++ {
		if !monthMap[int(cur.Month())] {
			cur = time.Date(cur.Year(), cur.Month()+1, 1, 0, 0, 0, 0, cur.Location())
			continue
		}
		if !domMap[cur.Day()] || !dowMap[int(cur.Weekday())] {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day()+1, 0, 0, 0, 0, cur.Location())
			continue
		}
		if !hourMap[cur.Hour()] {
			cur = time.Date(cur.Year(), cur.Month(), cur.Day(), cur.Hour()+1, 0, 0, 0, cur.Location())
			continue
		}
		if minMap[cur.Minute()] {
			return cur
		}
		cur = cur.Add(time.Minute)
	}

	return time.Time{}
}

// String returns the cron expression string.
func (s *Schedule) String() string {
	return s.expr
}
