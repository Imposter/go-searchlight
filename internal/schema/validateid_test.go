package schema

import "testing"

func TestValidateIDRefusesPathSteps(t *testing.T) {
	for _, id := range []string{".", ".."} {
		if err := ValidateID(id); err == nil {
			t.Errorf("ValidateID(%q): want an error", id)
		}
	}
	for _, id := range []string{"...", ".a", "a.", "a/..", "x"} {
		if err := ValidateID(id); err != nil {
			t.Errorf("ValidateID(%q) = %v", id, err)
		}
	}
}
