package main

import (
	"math/rand"
	"strings"
	"time"
)

// GeneratePayload creates a synthetic test payload of the given byte size.
func GeneratePayload(size int) string {
	if size <= 0 {
		return "echo bench"
	}
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return "echo " + string(b)
}

// ParseSchedulers parses and trims comma-separated scheduler addresses.
func ParseSchedulers(s string) []string {
	var addrs []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			addrs = append(addrs, a)
		}
	}
	return addrs
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
