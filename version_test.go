package spooler

import (
	"testing"

	"golang.org/x/mod/semver"
)

func TestVersion(t *testing.T) {
	if !semver.IsValid(Version) {
		t.Fatalf(
			"Version %q: want a semantic version with the v prefix",
			Version,
		)
	}
	// IsValid also takes the shorthands v1 and v1.2; a release tag carries
	// all three numbers, which is the canonical form plus any build suffix.
	if act, exp := Version, semver.Canonical(Version)+semver.Build(Version); act != exp {
		t.Errorf(
			"Version: %q; want %q",
			act, exp,
		)
	}
}
