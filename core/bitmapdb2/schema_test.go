package bitmapdb2

import (
	"math/rand"
	"testing"
)

func TestContainer(t *testing.T) {
	hi := 14
	base := uint32(hi) << 16
	c := NewEmptyContainer(uint16(hi))

	seed := int64(999777700)
	r := rand.New(rand.NewSource(seed))
	var numbers []uint16
	for i := 0; i < 8000; i++ {
		v := r.Intn(1 << 16)
		numbers = append(numbers, uint16(v))
	}

	for i, v := range numbers {
		c.Add(base + uint32(v))

		for j := 0; j <= i; j++ {
			n := base + uint32(numbers[j])
			if !c.Contains(n) {
				t.Fatalf("container not contains %d", n)
			}
		}
	}
}
