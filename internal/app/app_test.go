package app

import (
	"context"
	"testing"
)

func TestRunWithoutConfigReturnsError(t *testing.T) {
	if err := Run(context.Background(), nil); err == nil {
		t.Fatal("nil config started application")
	}
}
