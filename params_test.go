package noise_test

import (
	"github.com/guyu96/noise"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestDefaultParams(t *testing.T) {
	t.Parallel()

	p := noise.DefaultParams()
	assert.NotNil(t, p.Metadata)
	assert.True(t, p.MaxMessageSize > 0)
	assert.NotNil(t, p.Transport)
}
