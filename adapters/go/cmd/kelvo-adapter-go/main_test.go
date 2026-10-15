package main

import "testing"

func TestParseArgs(t *testing.T) {
	for _, tc := range []struct {
		args []string
		mode string
		pool int
		ok   bool
	}{
		{[]string{"worker"}, "worker", 4, true},
		{[]string{"serve"}, "serve", 4, true},
		{[]string{"serve", "--pool-size", "8"}, "serve", 8, true},
		{[]string{"serve", "--pool-size", "1"}, "serve", 1, true},
		{[]string{"serve", "--pool-size", "32"}, "serve", 32, true},
		{[]string{"serve", "--pool-size", "0"}, "", 0, false},
		{[]string{"serve", "--pool-size", "33"}, "", 0, false},
		{[]string{"serve", "--pool-size", "x"}, "", 0, false},
		{[]string{"serve", "--pool-size"}, "", 0, false},
		{[]string{"serve", "--other", "1"}, "", 0, false},
		{[]string{"worker", "--pool-size", "8"}, "", 0, false},
		{[]string{}, "", 0, false},
		{[]string{"other"}, "", 0, false},
	} {
		mode, pool, err := parseArgs(tc.args)
		if (err == nil) != tc.ok || mode != tc.mode || pool != tc.pool {
			t.Errorf("%v: got %q %d %v", tc.args, mode, pool, err)
		}
	}
}
