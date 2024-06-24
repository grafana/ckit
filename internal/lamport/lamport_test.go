package lamport

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClock_Now(t *testing.T) {
	tt := []struct {
		c      *Clock
		expect Time
	}{
		{
			c:      newClock(),
			expect: Time(0),
		},
		{
			c:      newClockWithInitialValue(),
			expect: Time(15),
		},
	}

	for i, tc := range tt {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			actual := tc.c.Now()
			require.Equal(t, tc.expect, actual)
		})
	}
}

func TestClock_Tick(t *testing.T) {
	tt := []struct {
		c      *Clock
		expect Time
	}{
		{
			c:      newClock(),
			expect: Time(1),
		},
		{
			c:      newClockWithInitialValue(),
			expect: Time(16),
		},
	}

	for i, tc := range tt {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			actual := tc.c.Tick()
			require.Equal(t, tc.expect, actual)
		})
	}
}

func TestClock_Observe(t *testing.T) {
	tt := []struct {
		c       *Clock
		observe Time
		expect  Time
	}{
		{
			c:       newClock(),
			observe: Time(0),
			expect:  Time(1),
		},
		{
			c:       newClockWithInitialValue(),
			observe: Time(20),
			expect:  Time(21),
		},
		{
			c:       newClockWithInitialValue(),
			observe: Time(5),
			expect:  Time(15),
		},
	}

	for i, tc := range tt {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			tc.c.Observe(tc.observe)
			actual := tc.c.Now()
			require.Equal(t, tc.expect, actual)
		})
	}
}

func BenchmarkClock_Now(b *testing.B) {
	c := newClockWithInitialValue()

	for i := 0; i < b.N; i++ {
		c.Now()
	}
}

func BenchmarkClock_Tick(b *testing.B) {
	c := newClockWithInitialValue()

	for i := 0; i < b.N; i++ {
		c.Tick()
	}
}

func BenchmarkClock_Observe(b *testing.B) {
	var c Clock

	for i := 0; i < b.N; i++ {
		c.Observe(Time(i))
	}
}

func newClock() *Clock {
	return &Clock{}
}

func newClockWithInitialValue() *Clock {
	c := newClock()
	c.time.Store(15)
	return c
}
