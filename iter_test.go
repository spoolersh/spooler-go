package spooler

import (
	"slices"
	"testing"
)

func TestListHelper(t *testing.T) {
	pages := map[string]Page[string]{
		"": {
			Items: []string{"a"},
			Next:  "a",
		},
		"a": {
			Items: []string{"b"},
		},
	}
	seq := list("", func(after string) (Page[string], error) {
		return pages[after], nil
	})
	for pass := range 2 {
		var act []string
		for item, err := range seq {
			if err != nil {
				t.Fatal(err)
			}
			act = append(act, item)
		}
		if exp := []string{"a", "b"}; !slices.Equal(act, exp) {
			t.Errorf(
				"pass %d: items: %v; want %v",
				pass+1,
				act,
				exp,
			)
		}
	}
}
