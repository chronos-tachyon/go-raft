package util

import (
	"sort"
	"testing"
)

func TestByVersion(t *testing.T) {
	actual := []string{"1.02", "1.1", "1.10", "0.99", "1.10.5"}
	expected := []string{"0.99", "1.1", "1.02", "1.10", "1.10.5"}
	sort.Sort(ByVersion(actual))
	if !equalStrings(actual, expected) {
		t.Errorf("expected %v, got %v", expected, actual)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
