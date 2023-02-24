package internal_test

import (
	"testing"

	"github.com/filecoin-project/lassie/pkg/internal/stream/internal"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSubscriptionAdd(t *testing.T) {
	t.Run("unsubscribes children", func(t *testing.T) {
		main := internal.NewSubscription(nil)
		var isCalled bool
		child := types.OnTearDown(func() error {
			isCalled = true
			return nil
		})
		main.Add(child)
		err := main.TearDown()
		require.NoError(t, err)
		require.True(t, isCalled)
	})

	t.Run("unsubscribes children immediately if already unsubscribed", func(t *testing.T) {
		main := internal.NewSubscription(nil)
		err := main.TearDown()
		require.NoError(t, err)
		var isCalled bool
		child := types.OnTearDown(func() error {
			isCalled = true
			return nil
		})
		main.Add(child)
		require.True(t, isCalled)
	})
}

func TestSubscriptionRemove(t *testing.T) {
	var isCalled bool
	main := internal.NewSubscription(nil)
	child := types.OnTearDown(func() error {
		isCalled = true
		return nil
	})
	main.Add(child)
	main.Remove(child)
	err := main.TearDown()
	require.NoError(t, err)
	require.False(t, isCalled)
}
