package config

import "testing"

// The shipped examples must stay loadable.
func TestExampleConfigsLoad(t *testing.T) {
	t.Setenv("S3FLOW_ADMIN_TOKEN", "0123456789abcdef0123")
	t.Setenv("S3FLOW_CLUSTER_TOKEN", "0123456789abcdef0123")
	for _, f := range []string{"../../config.example.json", "../../config.environments.example.json"} {
		if _, err := Load(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
