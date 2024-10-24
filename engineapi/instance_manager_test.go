package engineapi

import (
	"testing"
)

func TestGetReplicaAddresses(t *testing.T) {
	tests := []struct {
		name             string
		replicaAddresses map[string]string
		initiatorIP      string
		targetIP         string
		expected         map[string]string
		expectError      bool
	}{
		{
			name: "No filtering needed",
			replicaAddresses: map[string]string{
				"replica1": "192.168.1.1:9502",
				"replica2": "192.168.1.2:9502",
			},
			initiatorIP: "192.168.1.3",
			targetIP:    "192.168.1.3",
			expected: map[string]string{
				"replica1": "192.168.1.1:9502",
				"replica2": "192.168.1.2:9502",
			},
			expectError: false,
		},
		{
			name: "Filter out initiator IP",
			replicaAddresses: map[string]string{
				"replica1": "192.168.1.1:9502",
				"replica2": "192.168.1.2:9502",
			},
			initiatorIP: "192.168.1.1",
			targetIP:    "192.168.1.3",
			expected: map[string]string{
				"replica2": "192.168.1.2:9502",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getReplicaAddresses(tt.replicaAddresses, tt.initiatorIP, tt.targetIP)
			if (err != nil) != tt.expectError {
				t.Errorf("expected error: %v, got: %v", tt.expectError, err)
			}
			if !tt.expectError && !equalMaps(result, tt.expected) {
				t.Errorf("expected: %v, got: %v", tt.expected, result)
			}
		})
	}
}

func equalMaps(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
