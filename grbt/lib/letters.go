package grbt

import (
	"fmt"
	"math"
	"sync"
)

// ArcType is the kind of circle we're getting out of this.
type ArcType int

// Arcs of varying completion
const (
	ArcCircle = ArcType(iota)
	ArcSemi
	ArcQuarter
	ArcThreeQuarter
)

// Dir indicates a cardinal direction
type Dir int

// Directions
const (
	DirRight = Dir(iota)
	DirDown
	DirLeft
	DirUp
)

// Arc represents a limited configurations of parts of a circle
type Arc struct {
	Center Vec
	T      ArcType
	D      Dir
	R      Float
}

// Letter models character with CSG.
// Separate Positions I think. That will get subtracted from the
// ray's position.
type Letter struct {
	Char   string
	Lines  [][2][2]Float
	Curves []Arc

	aabb [2]Vec
	init sync.Once
}

// Initialize sets the bounding box for the letter.
func (m *Letter) Initialize() {
	m.init.Do(func() {
		xMin, xMax := Float(math.MaxFloat32), Float(-math.MaxFloat32)
		yMin, yMax := Float(math.MaxFloat32), Float(-math.MaxFloat32)
		for _, line := range m.Lines {
			xMin = Min(xMin, line[0][0])
			yMin = Min(yMin, line[0][1])
			xMin = Min(xMin, line[1][0])
			yMin = Min(yMin, line[1][1])

			xMax = Max(xMax, line[0][0])
			yMax = Max(yMax, line[0][1])
			xMax = Max(xMax, line[1][0])
			yMax = Max(yMax, line[1][1])
		}

		// How much the curves adds depend on whether they're half circles
		// or not.
		for _, arc := range m.Curves {
			if !(arc.T == ArcSemi && arc.D == DirRight) {
				xMin = Min(xMin, arc.Center.X-arc.R)
			}
			if !(arc.T == ArcSemi && arc.D == DirLeft) {
				xMax = Max(xMax, arc.Center.X+arc.R)
			}
			if !(arc.T == ArcSemi && arc.D == DirUp) {
				yMin = Min(yMin, arc.Center.Y-arc.R)
			}
			if !(arc.T == ArcSemi && arc.D == DirDown) {
				yMax = Max(yMax, arc.Center.Y+arc.R)
			}
		}

		// Letters are about 1 unit thick each.
		m.aabb[0] = Vec{X: xMin - 0.5, Y: yMin - 0.5, Z: -0.5}
		m.aabb[1] = Vec{X: xMax + 0.5, Y: yMax + 0.5, Z: 0.5}
	})
}

// Width returns the unit width of the letters.
func (m *Letter) Width() Float {
	return m.aabb[1].X - m.aabb[0].X
}

// Query checks if we hit/are in this letter.
func (m *Letter) Query(position Vec) (Float, HitType) {
	distance := Float(1e9)
	// If the position is ~close to or in~ the bounding box, we
	// check where it's actually close to, and return that instead.
	// Fun fact! If the rest of this isn't implemented,
	// the bounding box is what gets returned!
	if d := BoxTest(position, m.aabb[0], m.aabb[1]); d > epsilon {
		return d, HitAABB
	}
	f := position // Flattened position (z=0)
	f.Z = 0
	for _, line := range m.Lines {
		// The original from the letter encoding.
		// begin := Vec{X: toi(line[0]), Y: toi(line[1])}.Times(MonoVec(0.5))
		// e := Vec{X: toi(line[2]), Y: toi(line[3])}.Times(MonoVec(0.5)).Minus(begin)

		begin := Vec{X: line[0][0], Y: line[0][1]}
		e := Vec{X: line[1][0], Y: line[1][1]}.Minus(begin)
		// I think something here is what's causing the rounding of the letters somehow.
		o1 := begin.Minus(f)
		o2 := MonoVec(float64(1 / e.Dot(e)))
		o3 := MonoVec(
			float64(Min(
				-Min(o1.Dot(e.Times(o2)),
					0),
				1),
			),
		)
		o4 := begin.Plus(e.Times(o3))
		o := f.Minus(o4)
		distance = Min(distance, o.Dot(o)) // compare squared distance.
	}
	distance = Float(math.Sqrt(float64(distance))) // Get real distance, not square distance.

	// Fixed radius 2 arc curves.
	// TODO Determine a better way to specify these.
	for _, arc := range m.Curves {
		// Get the vector from the flattened position
		// to the center of the circle.
		o := f.Minus(arc.Center)
		var d2 float64
		radius := arc.R

		switch arc.T {
		case ArcCircle:
			d2 = math.Abs(math.Sqrt(float64(o.Dot(o))) - float64(radius))
		case ArcQuarter:
		case ArcSemi:
			// This handles a left half curve.
			switch arc.D {
			case DirRight:
				if o.X > 0 {
					d2 = math.Abs(math.Sqrt(float64(o.Dot(o))) - float64(radius))
				} else {
					if o.Y > 0 {
						o.Y += -radius
					} else {
						o.Y += radius
					}
					d2 = math.Sqrt(float64(o.Dot(o)))
				}
			case DirLeft:
				if o.X < 0 {
					d2 = math.Abs(math.Sqrt(float64(o.Dot(o))) - float64(radius))
				} else {
					if o.Y > 0 {
						o.Y += -radius
					} else {
						o.Y += radius
					}
					d2 = math.Sqrt(float64(o.Dot(o)))
				}
			case DirDown:
				if o.Y < 0 {
					d2 = math.Abs(math.Sqrt(float64(o.Dot(o))) - float64(radius))
				} else {
					if o.X > 0 {
						o.X += -radius
					} else {
						o.X += radius
					}
					d2 = math.Sqrt(float64(o.Dot(o)))
				}
			case DirUp:
				if o.Y > 0 {
					d2 = math.Abs(math.Sqrt(float64(o.Dot(o))) - float64(radius))
				} else {
					if o.X > 0 {
						o.X += -radius
					} else {
						o.X += radius
					}
					d2 = math.Sqrt(float64(o.Dot(o)))
				}
			}
		case ArcThreeQuarter:
			// Not yet implemented.
		}
		distance = Min(distance, Float(d2))
	}
	// Removing this causes a single giant mirror, so it seems like this controls the "depth" of
	// the letters. This should have been obvious from the position.Z parameter.
	distance = Float(math.Pow(math.Pow(float64(distance), 8)+math.Pow(float64(position.Z), 8), 0.125) - 0.5)

	return distance, HitLetterRed
}

// ValidateWord checks that the word can be printed by the letter models.
func ValidateWord(word string) error {
	for _, c := range word {
		_, ok := letterModels[string(c)]
		if !ok {
			return fmt.Errorf("word %q contains unknown character %q", word, string(c))
		}
	}
	return nil
}

var letterModels = map[string]*Letter{
	" ": &Letter{Char: " ",
		Lines: [][2][2]Float{
			{{0, -4}, {4, -4}}, // (space is under the map)
		},
	},
	"A": &Letter{Char: "A",
		Lines: [][2][2]Float{
			{{0, 0}, {2, 8}}, // /
			{{2, 8}, {4, 0}}, // \
			{{1, 4}, {3, 4}}, // -
		},
	},
	"B": &Letter{Char: "B",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 0}, {2, 0}}, // bottom -
			{{0, 4}, {2, 4}}, // Mid -
			{{0, 8}, {2, 8}}, // Top -
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirRight, 2}, // top
			{Vec{2, 2, 0}, ArcSemi, DirRight, 2}, // bottom
		},
	},
	"C": &Letter{Char: "C",
		Lines: [][2][2]Float{
			{{0, 2}, {0, 6}}, // left |
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirUp, 2},
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	},
	"D": &Letter{Char: "D",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 0}, {1, 0}}, // top -
			{{0, 8}, {1, 8}}, // down -
		},
		Curves: []Arc{
			{Vec{1, 4, 0}, ArcSemi, DirRight, 4},
		},
	},
	"E": &Letter{Char: "E",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 0}, {3, 0}}, // bottom -
			{{0, 4}, {2, 4}}, // Mid -
			{{0, 8}, {3, 8}}, // Top -
		},
	},
	"F": &Letter{Char: "F",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 4}, {2, 4}}, // Mid -
			{{0, 8}, {3, 8}}, // Top -
		},
	},
	"G": &Letter{Char: "G",
		Lines: [][2][2]Float{
			{{4, 0}, {5, 0}},       // low -
			{{5, 0}, {5, 3.5}},     // |
			{{3.5, 3.5}, {5, 3.5}}, // mid -
			{{4, 8}, {5, 8}},       // top -
		},
		Curves: []Arc{
			{Vec{4, 4, 0}, ArcSemi, DirLeft, 4},
		},
	},
	"H": &Letter{Char: "H",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // left |
			{{4, 0}, {4, 8}}, // right |
			{{0, 4}, {4, 4}}, //  -
		},
	},
	"I": &Letter{Char: "I",
		Lines: [][2][2]Float{
			{{0, 0}, {2, 0}}, // Bottom -
			{{1, 0}, {1, 8}}, // |
			{{0, 8}, {2, 8}}, // Top -
		},
	},
	"J": &Letter{Char: "J",
		Lines: [][2][2]Float{
			{{4, 2}, {4, 8}}, // |
			{{2, 8}, {4, 8}}, // Top -
		},
		Curves: []Arc{
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	},
	"K": &Letter{Char: "K",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 4}, {4, 8}}, // /
			{{0, 4}, {4, 0}}, // \
		},
	},
	"L": &Letter{Char: "L",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 0}, {3, 0}}, // bottom -
		},
	},
	"M": &Letter{Char: "M",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // left |
			{{0, 8}, {2, 4}}, // bottom -
			{{2, 4}, {4, 8}}, // bottom -
			{{4, 0}, {4, 8}}, // right |
		},
	},
	"N": &Letter{Char: "N",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // left |
			{{0, 8}, {4, 0}}, // \
			{{4, 0}, {4, 8}}, // right |
		},
	},
	"O": &Letter{Char: "O",
		Lines: [][2][2]Float{
			{{0, 2}, {0, 6}}, // left |
			{{4, 2}, {4, 6}}, // right |
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirUp, 2},
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	},
	"P": &Letter{Char: "P",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 4}, {2, 4}}, // Mid -
			{{0, 8}, {2, 8}}, // Top -
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirRight, 2}, // top
		},
	},
	"Q": &Letter{Char: "Q",
		Lines: [][2][2]Float{
			{{0, 2}, {0, 6}}, // left |
			{{4, 2}, {4, 6}}, // right |
			{{2, 2}, {4, 0}}, // \
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirUp, 2},
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	},
	"R": &Letter{Char: "R",
		Lines: [][2][2]Float{
			{{0, 0}, {0, 8}}, // |
			{{0, 4}, {2, 4}}, // mid -
			{{0, 8}, {2, 8}}, // top -
			{{1, 4}, {4, 0}}, // \
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirRight, 2}, // top
		},
	},
	"S": &Letter{Char: "S",
		Curves: []Arc{ // TODO 2 ThreeQuarters instead?
			{Vec{2, 6, 0}, ArcSemi, DirUp, 2},
			{Vec{2, 6, 0}, ArcSemi, DirLeft, 2},
			{Vec{2, 2, 0}, ArcSemi, DirRight, 2},
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	},
	"T": {Char: "T",
		Lines: [][2][2]Float{
			{{2, 0}, {2, 8}}, // |
			{{0, 8}, {4, 8}}, // Top -
		},
	},
	"U": &Letter{Char: "U",
		Lines: [][2][2]Float{
			{{0, 2}, {0, 8}}, // left |
			{{4, 2}, {4, 8}}, // right |
		},
		Curves: []Arc{
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	},
	"V": &Letter{Char: "V",
		Lines: [][2][2]Float{
			{{0, 8}, {2, 0}}, // \
			{{2, 0}, {4, 8}}, // /
		},
	},
	"W": &Letter{Char: "W",
		Lines: [][2][2]Float{
			{{0, 8}, {2, 0}}, // \
			{{2, 0}, {4, 4}}, // /
			{{4, 4}, {6, 0}}, // \
			{{6, 0}, {8, 8}}, // /
		},
	},
	"X": &Letter{Char: "X",
		Lines: [][2][2]Float{
			{{0, 0}, {4, 8}}, // /
			{{0, 8}, {4, 0}}, // \
		},
	},
	"Y": &Letter{Char: "Y",
		Lines: [][2][2]Float{
			{{2, 4}, {4, 8}}, // /
			{{0, 8}, {2, 4}}, // \
			{{2, 0}, {2, 4}}, // |
		},
	},
	"Z": &Letter{Char: "Z",
		Lines: [][2][2]Float{
			{{0, 0}, {4, 0}}, // bottom -
			{{0, 0}, {4, 8}}, // /
			{{0, 8}, {4, 8}}, // top -
		},
	},
	"0": &Letter{Char: "0",
		Lines: [][2][2]Float{
			{{0, 2}, {0, 6}}, // left |
			{{2, 3}, {2, 5}}, // mid |
			{{4, 2}, {4, 6}}, // right |
		},
		Curves: []Arc{
			{Vec{2, 6, 0}, ArcSemi, DirUp, 2},
			{Vec{2, 2, 0}, ArcSemi, DirDown, 2},
		},
	}, // 1 2 3 4 5 6 7
	"8": &Letter{Char: "8",
		Curves: []Arc{
			{Center: Vec{2, 2, 0}, T: ArcCircle, R: 2},
			{Center: Vec{2, 6, 0}, T: ArcCircle, R: 2},
		},
	}, // 9
	"#": &Letter{Char: "#",
		Lines: [][2][2]Float{
			{{0, 2}, {5, 2}}, // bottom -
			{{1, 6}, {6, 6}}, // top -
			{{1, 0}, {3, 8}}, // /
			{{3, 0}, {5, 8}}, // /
		},
	},
}
