package beam

import "testing"

func FuzzSamplerState(f *testing.F) {
	f.Add(uint8(0), uint16(0), uint16(0))
	f.Add(uint8(1), uint16(1), uint16(1))
	f.Add(uint8(2), uint16(2), uint16(0))
	f.Add(uint8(1), uint16(3), uint16(23))
	f.Add(uint8(1), uint16(42), uint16(170))
	f.Add(uint8(2), uint16(16383), uint16(1<<15))

	f.Fuzz(func(t *testing.T, a uint8, b uint16, c uint16) {
		mets := newMetricsStore(0)
		phase := uint32(a % 3)
		transition := uint32(b % 16384)
		edge := uint32(c % 0xFFFF)

		mets.storeState(phase, transition, edge)
		cur := mets.curState()

		if got, want := cur.phase, uint32(phase); got != want {
			t.Errorf("incorrect state phase: got %v, want %v", got, want)
		}
		if got, want := cur.edge, edgeIndex(edge); got != want {
			t.Errorf("incorrect state edge: got %v, want %v", got, want)
		}
		if got, want := cur.transition, uint32(transition); got != want {
			t.Errorf("incorrect state transition: got %v, want %v", got, want)
		}
	})
}
