package timeutil_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sxwebdev/xutils/timeutil"
)

func TestTimeIt_MeasuresAndReturnsNilError(t *testing.T) {
	d, err := timeutil.TimeIt(func() error {
		time.Sleep(20 * time.Millisecond)
		return nil
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, d, 20*time.Millisecond, "must measure at least the sleep duration")
	assert.Less(t, d, time.Second, "measurement should be in the right ballpark")
}

func TestTimeIt_PropagatesError(t *testing.T) {
	sentinel := errors.New("boom")
	d, err := timeutil.TimeIt(func() error { return sentinel })
	require.ErrorIs(t, err, sentinel, "fn's error must be propagated")
	assert.GreaterOrEqual(t, d, time.Duration(0))
}
