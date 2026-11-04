package cron

import (
	"testing"
	"time"
)

func TestParseValidExpressions(t *testing.T) {
	valid := []string{
		"* * * * *",
		"*/5 * * * *",
		"0 12 * * *",
		"30 8 1 1 *",
		"0 0 * * 0",
	}
	for _, expr := range valid {
		s, err := Parse(expr)
		if err != nil {
			t.Errorf("expected %q to be valid, got: %v", expr, err)
		}
		if s == nil {
			t.Errorf("expected non-nil schedule for %q", expr)
		}
	}
}

func TestParseInvalidExpressions(t *testing.T) {
	invalid := []string{
		"",
		"* * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 32 * *",
		"* * * 13 *",
		"* * * * 7",
		"*/0 * * * *",
		"not-a-cron",
	}
	for _, expr := range invalid {
		_, err := Parse(expr)
		if err == nil {
			t.Errorf("expected %q to be invalid, got nil error", expr)
		}
	}
}

func TestNextCalculation(t *testing.T) {
	s, err := Parse("*/15 * * * *")
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// 10:02 -> next should be 10:15
	base := time.Date(2026, 11, 4, 10, 2, 0, 0, time.UTC)
	next := s.Next(base)
	want := time.Date(2026, 11, 4, 10, 15, 0, 0, time.UTC)
	if !next.Equal(want) {
		t.Errorf("next mismatch: got %v, want %v", next, want)
	}

	// 10:15 -> next should be 10:30
	next2 := s.Next(next)
	want2 := time.Date(2026, 11, 4, 10, 30, 0, 0, time.UTC)
	if !next2.Equal(want2) {
		t.Errorf("next mismatch: got %v, want %v", next2, want2)
	}
}
