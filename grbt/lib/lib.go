// Package grbt contains code for a ray tracer in beam.
package grbt

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
)

// Float is a convenience type incase I decide to
// change the precision later.
type Float float64

// Min returns the minimum of two Floats.
func Min(l, r Float) Float {
	if l < r {
		return l
	}
	return r
}

// Max returns the maximum of two Floats.
func Max(l, r Float) Float {
	if l > r {
		return l
	}
	return r
}

// Vec is a vector of 3 Floats.
type Vec struct {
	X, Y, Z Float
}

// MonoVec produces a vector with all the values set to v.
func MonoVec(v float64) Vec {
	return Vec{Float(v), Float(v), Float(v)}
}

// Plus adds two vectors together.
func (v Vec) Plus(r Vec) Vec {
	return Vec{v.X + r.X, v.Y + r.Y, v.Z + r.Z}
}

// Minus subtracts r from v.
func (v Vec) Minus(r Vec) Vec {
	return Vec{v.X - r.X, v.Y - r.Y, v.Z - r.Z}
}

// Times multiplies vectors together.
func (v Vec) Times(r Vec) Vec {
	return Vec{v.X * r.X, v.Y * r.Y, v.Z * r.Z}
}

// Dot calculates the dot product of two vectors.
func (v Vec) Dot(r Vec) Float {
	return v.X*r.X + v.Y*r.Y + v.Z*r.Z
}

// Dot2 performs a dot product against v itself.
func (v Vec) Dot2() Float {
	return v.Dot(v)
}

// Cross calculates the Cross product of two vectors.
func (v Vec) Cross(b Vec) Vec {
	return Vec{
		v.Y*b.Z - v.Z*b.Y,
		v.Z*b.X - v.X*b.Z,
		v.X*b.Y - v.Y*b.X,
	}
}

// InvSqrt returns the inverse square root.
func (v Vec) InvSqrt() Vec {
	selfDot := v.Dot(v)
	val := Float(1 / math.Sqrt(float64(selfDot)))
	return Vec{v.X * val, v.Y * val, v.Z * val}
}

// ImageConfig contains properties of the generated image, used to generate initial rays.
type ImageConfig struct {
	Width, Height    float64
	Samples, Bounces int64

	Goal, Left, Up Vec
}

// RandomVal returns a random float64 between 0 and 1.
func RandomVal() float64 {
	return rand.Float64()
}

// BoxTest is the Rectangle CSG equation.
// Returns minimum signed distance from space carved by lowerLeft vertex
// and opposite rectangle vertex upperRight.
func BoxTest(position, lowerLeft, upperRight Vec) Float {
	lowerLeft = position.Minus(lowerLeft)
	upperRight = upperRight.Minus(position)
	return -Min(
		Min(Min(lowerLeft.X, upperRight.X), Min(lowerLeft.Y, upperRight.Y)),
		Min(lowerLeft.Z, upperRight.Z),
	)
}

// SphereTest is the Sphere CSG equation.
func SphereTest(position, center Vec, radius Float) Float {
	delta := center.Minus(position)
	distance := math.Sqrt(float64(delta.Dot(delta)))
	return Float(distance) - radius
}

// HitType is the material being struck by the ray.
type HitType int

// HitTypes for known materials.
const (
	HitNone = HitType(iota)
	HitLetterBlack
	HitLetterWhite
	HitLetterRed
	HitLetterGreen
	HitLetterBlue
	HitLetterYellow
	HitGopherBlue
	HitGopherTeal
	HitWall
	HitSun
	HitAABB
)

func toi(v byte) Float {
	return Float(int(v)-79) / 2
}

// Model is where we're going to hang all the geometry stuff.
type Model interface {
	// Initialize does any necessary precomputations on the model
	// eg. Handle AABBs? How do we propagate those up the scene graph?
	Initialize()

	// Query calculates the distance to the model from the position.
	Query(position Vec) (distance Float, material HitType)

	// Keeping HitType/Material returned from the query because then the model
	// can have mixed surfaces depending on
	// what gets hit.

	// Move to another interface and type assert?
	// Compute in world space? (but then I'm storing that everywhere, instead of doing simple subtraction)
	// QueryAABB calculates the distance to the axis aligned bounding box model from the position
	// QueryAABB(position Vec) (distance Float)
}

// Room is the room that contains the scene.
type Room struct {
}

// Initialize is a No-op for Room.
func (m *Room) Initialize() {}

// Query is a checks colliding with the room.
func (m *Room) Query(position Vec) (Float, HitType) {
	var roomDist Float
	roomDist = Min( // min(A,B) = Union with Constructive solid geometry
		//-min carves an empty space
		-Min( // Lower room
			BoxTest(position, Vec{-30, -.5, -30}, Vec{30, 18, 30}),
			// Upper room
			BoxTest(position, Vec{-25, 17, -25}, Vec{25, 20, 25}),
		),
		BoxTest( // Ceiling "planks" spaced 8 units apart.
			Vec{Float(math.Mod(math.Abs(float64(position.X)), 8)),
				position.Y,
				position.Z},
			Vec{1.5, 18.5, -25},
			Vec{6.5, 20, 25},
		),
	)
	return roomDist, HitWall
}

// Obj represents a single object set of connected polygons.
type Obj struct {
	Vertices []Vec
	Faces    [][4]uint32

	fnormals []Vec
	aabb     [2]Vec
	init     sync.Once
}

// Initialize precomputes information from the data.
func (m *Obj) Initialize() {
	m.init.Do(func() {
		xMin, xMax := Float(math.MaxFloat32), Float(-math.MaxFloat32)
		yMin, yMax := Float(math.MaxFloat32), Float(-math.MaxFloat32)
		zMin, zMax := Float(math.MaxFloat32), Float(-math.MaxFloat32)
		// Skip the first one since none of the faces will use it.
		for _, v := range m.Vertices[1:] {
			xMin = Min(xMin, v.X)
			yMin = Min(yMin, v.Y)
			zMin = Min(zMin, v.Z)

			xMax = Max(xMax, v.X)
			yMax = Max(yMax, v.Y)
			zMax = Max(zMax, v.Z)
		}

		// Letters are about 1 unit thick each.
		m.aabb[0] = Vec{X: xMin, Y: yMin, Z: zMin}
		m.aabb[1] = Vec{X: xMax, Y: yMax, Z: zMax}
	})
}

// Clamp returns the value v, but restricted to the upper and lower bounds.
func Clamp(v, upper, lower Float) Float {
	return Max(upper, Min(v, lower))
}

// Query checks which triangle we're closest to.
func (m *Obj) Query(position Vec) (Float, HitType) {
	distance, hit := Float(1e9), HitNone
	if d := BoxTest(position, m.aabb[0], m.aabb[1]); d > epsilon {
		return d, HitAABB
	}
	for _, f := range m.Faces {
		// Most of the vectors could be precomputed.
		// from http://iquilezles.org/www/articles/distfunctions/distfunctions.htm
		// float udTriangle( vec3 p, vec3 a, vec3 b, vec3 c )
		p, a, b, c := position, m.Vertices[f[0]], m.Vertices[f[1]], m.Vertices[f[2]]

		ba := b.Minus(a)
		cb := c.Minus(b)
		ac := a.Minus(c)
		nor := ba.Cross(ac)

		pa := p.Minus(a)
		pb := p.Minus(b)
		pc := p.Minus(c)

		var t Float
		if math.Copysign(1, float64(ba.Cross(nor).Dot(pa)))+
			math.Copysign(1, float64(cb.Cross(nor).Dot(pb)))+
			math.Copysign(1, float64(ac.Cross(nor).Dot(pc))) <= 2.0 {
			t = Min(
				Min(
					(ba.Times(MonoVec(1.0/float64(Clamp(ba.Dot(pa)/ba.Dot2(), 0.0, 1.0)))).Minus(pa)).Dot2(),
					(cb.Times(MonoVec(1.0/float64(Clamp(cb.Dot(pb)/cb.Dot2(), 0.0, 1.0)))).Minus(pb)).Dot2()),
				(ac.Times(MonoVec(1.0 / float64(Clamp(ac.Dot(pc)/ac.Dot2(), 0.0, 1.0)))).Minus(pc)).Dot2(),
			)
		} else {
			t = nor.Dot(pa) * nor.Dot(pa) / nor.Dot2()
		}
		t = Float(math.Sqrt(float64(t)))
		if t < distance {
			distance, hit = t, HitGopherBlue
		}
	}
	return distance, hit
}

// Sphere models a sphere.
type Sphere struct {
	Radius   Float
	Material HitType
}

// Initialize provides a default material if needed.
func (m *Sphere) Initialize() {
	if m.Material == HitNone {
		m.Material = HitGopherTeal
	}
}

// Query checks the distance of position from the Sphere.
func (m *Sphere) Query(position Vec) (Float, HitType) {
	return SphereTest(position, Vec{}, m.Radius), m.Material
}

// PositionedModel is a model with a "global" position with the scene.
// This is where rotations etc need to be handled to translate from
// world space to model space.
type PositionedModel struct {
	Pos Vec
	Model
}

// Query converts the position form World Space to Model space, and
// queries the model.
func (m *PositionedModel) Query(position Vec) (Float, HitType) {
	// Minus because we're moving the position to the model.
	return m.Model.Query(position.Minus(m.Pos))
}

// Scene is the what is being Ray Traced.
type Scene struct {
	Ms   []*PositionedModel
	init sync.Once
}

// Initialize recursively initializes the rest of the scene.
func (s *Scene) Initialize() {
	s.init.Do(func() {
		for _, m := range s.Ms {
			m.Initialize()
		}
	})
}

// Query samples the Scene and returns the closest collision.
func (s *Scene) Query(position Vec) (Float, HitType) {
	// Samples the world using Signed Distance Fields.
	distance := Float(1e9)

	hitType := HitNone
	for _, m := range s.Ms {
		d, h := m.Query(position)
		if d < distance+epsilon {
			distance, hitType = d, h
		}
	}

	sun := 19.9 - position.Y // Everything above 19.9 is light source.
	if sun < distance {
		distance = sun
		hitType = HitSun
	}

	return distance, hitType
}

// RayMarching performs signed sphere marching
// Returns hitType and updated hit positions & normal.
func RayMarching(origin, direction Vec, scene *Scene) (hitType HitType, hitPos, hitNorm Vec) {
	hitType = HitNone
	noHitCount := 0
	var d Float // distance from closest object in world.

	// Signed distance marching (if distance traveled is less than epsilon,
	// we hit something and should produce a normal for it.
	for totalD := Float(0); totalD < maxDistance; totalD += d {
		hitPos = origin.Plus(direction.Times(MonoVec(float64(totalD))))
		d, hitType = scene.Query(hitPos)
		if d < epsilon || noHitCount > 99 {
			// TODO This can probably be cheaper if I'm not checking the whole scene and just
			// the model being struck. Instead of HitType , Query should probably return
			// the specific model hit, then the material will be hung off of that. (PositionedModel that is.)
			// There are cheaper ways of calculating the normal of something too.
			x, _ := scene.Query(hitPos.Plus(Vec{epsilon, 0, 0}))
			y, _ := scene.Query(hitPos.Plus(Vec{0, epsilon, 0}))
			z, _ := scene.Query(hitPos.Plus(Vec{0, 0, epsilon}))

			hitNorm = Vec{x - d,
				y - d,
				z - d}.InvSqrt()

			return hitType, hitPos, hitNorm
		}
		noHitCount++
	}
	return HitNone, hitPos, hitNorm
}

var (
	colourFactor = MonoVec(float64(1) / float64(255))
	black        = MonoVec(0.2)
)

// Trace casts rays, and handles bounces, returning the colour.
func Trace(origin, direction Vec, scene *Scene, maxBounces int64) Vec {
	var hitType HitType
	// It's important that these are initialized to 1 to start.
	sampledPosition, normal, attenuation := MonoVec(1), MonoVec(1), MonoVec(1)
	color := MonoVec(0)                        // This actually makes it black to start. Maybe better with bounces now?
	lightDirection := Vec{.6, .6, 1}.InvSqrt() // Directional light

	for bounceCount := int64(0); bounceCount < maxBounces; bounceCount++ {
		hitType, sampledPosition, normal = RayMarching(origin, direction, scene)
		switch hitType {
		case HitNone:
			// No hit. This is over, return color, no op.
			return color
		case HitLetterRed, HitLetterGreen, HitLetterYellow, HitLetterBlue, HitLetterBlack, HitLetterWhite: // Specular bounce on a letter. No color acc.
			var albedo Vec
			switch hitType {
			case HitLetterBlack:
				albedo = black
			case HitLetterWhite:
				albedo = Vec{250, 250, 250}.Times(colourFactor)
			case HitLetterRed:
				albedo = Vec{250, 0, 0}.Times(colourFactor)
			case HitLetterGreen:
				albedo = Vec{0, 250, 0}.Times(colourFactor)
			case HitLetterBlue:
				albedo = Vec{0, 0, 250}.Times(colourFactor)
			case HitLetterYellow:
				albedo = Vec{250, 250, 0}.Times(colourFactor)
			}
			direction = direction.Plus(normal.Times(MonoVec(float64(normal.Dot(direction) * -2))))
			origin = sampledPosition.Plus(direction.Times(MonoVec(0.1)))
			attenuation = attenuation.Times(albedo) // attenuation.Times(MonoVec(0.2)) // Attenuation via distance traveled.
		case HitGopherBlue, HitGopherTeal, HitWall:
			var albedo Vec
			switch hitType {
			case HitGopherBlue:
				albedo = Vec{52, 152, 219}.Times(colourFactor)
			case HitGopherTeal:
				albedo = Vec{23, 165, 137}.Times(colourFactor)
			case HitWall:
				albedo = MonoVec(0.2) // .Times(MonoVec(1 / math.Pi))
			}
			incidence := normal.Dot(lightDirection)
			// Tau times a random value, because diffuse
			phi := 6.283185 * RandomVal()
			c := RandomVal()
			sinTheta := math.Sqrt(float64(1 - c))
			g := Float(1)
			if normal.Z < 0 {
				g = -1
			}
			u := -1 / (g + normal.Z)
			v := normal.X * normal.Y * u

			d1 := Vec{v,
				g + normal.Y*normal.Y*u,
				-normal.Y}.Times(MonoVec(math.Cos(phi) * sinTheta))
			d2 := Vec{1 + g*normal.X*normal.X*u,
				g * v,
				-g * normal.X}.Times(MonoVec(math.Sin(phi) * sinTheta))
			d3 := normal.Times(MonoVec(math.Sqrt(c)))
			direction = d1.Plus(d2).Plus(d3)

			// Start the next ray just off the surface.
			origin = sampledPosition.Plus(direction.Times(MonoVec(0.1)))

			attenuation = attenuation.Times(albedo) // Attenuation via distance/bounces traveled.
			// Checks contribution of direct lights.
			if incidence > 0 && hitType != HitGopherBlue {
				var hitType HitType
				hitType, sampledPosition, normal = RayMarching(sampledPosition.Plus(normal.Times(MonoVec(.1))), lightDirection, scene)
				switch hitType {
				case HitSun:
					color = color.Plus(attenuation.Times(Vec{500, 400, 100}).Times(MonoVec(float64(incidence))))
				}
			}
		case HitSun:
			// Sun Color
			color = color.Plus(attenuation.Times(Vec{50, 80, 100}))
		case HitAABB:
			//direction = direction.Plus(normal.Times(MonoVec(float64(normal.Dot(direction) * -2))))
			origin = sampledPosition.Plus(direction.Times(MonoVec(0.1)))
			attenuation = attenuation.Times(MonoVec(0.2)) // Attenuation via distance traveled.
			color = color.Plus(attenuation.Times(Vec{50, 0, 0}))
		}
	}
	return color
}

const (
	epsilon     = 0.001
	maxDistance = 100 // 100 by default
)

func populateScene(word string) *Scene {
	scene := &Scene{}
	totalWidth := Float(0)
	for _, c := range word {
		if c == ' ' {
			totalWidth += 4 //? maybe 4?
			continue
		}
		l, ok := letterModels[string(c)]
		if !ok {
			panic(fmt.Sprintf("word %q contains unknown character %v", word, c))
		}
		l.Initialize()
		totalWidth += l.Width() + 1
	}
	startX := -((totalWidth - 1) / 2) + 0.5
	if startX < -16 {
		startX = -16 // To avoid going out of the screen to the left.
	}
	fmt.Fprintf(os.Stderr, "%v startX %v ", word, startX)
	for _, c := range word {
		if c == ' ' {
			startX += 4 //? maybe 4?
			continue
		}
		l := letterModels[string(c)]
		var mPos Vec
		// Assume characters are 6 units wide each, and 9 units tall, ~ centered around 0/
		// I is 3 wide usually.
		width := l.Width()
		fmt.Fprintf(os.Stderr, "%v - %v; ", string(c), width)
		mPos.X = startX
		startX += width + 1 // Move the start for the next character, +1 for spacing
		scene.Ms = append(scene.Ms, &PositionedModel{mPos, l})
	}
	scene.Ms = append(scene.Ms, &PositionedModel{Vec{}, &Room{}})
	// scene.Ms = append(scene.Ms, &PositionedModel{
	// 	Vec{X: -10, Y: 3, Z: 5},
	// 	&Obj{
	// 		Vertices: []Vec{{0, 0, -5}, {0, 0, 0}, {0, 0, 6}, {6, 0, 6}, {0, 6, 0}},
	// 		Faces:    [][4]uint32{{1, 2, 3, 0}, {1, 2, 4, 0}, {1, 3, 4, 0}, {2, 3, 4, 0}},
	// 	},
	// })
	scene.Ms = append(scene.Ms, &PositionedModel{
		Vec{X: 10, Y: 5.5, Z: -12},
		&Sphere{
			Radius:   6,
			Material: HitGopherBlue,
		},
	})
	scene.Ms = append(scene.Ms, &PositionedModel{
		Vec{X: -6, Y: 3.5, Z: -16},
		&Sphere{
			Radius:   4,
			Material: HitLetterGreen,
		},
	})
	scene.Ms = append(scene.Ms, &PositionedModel{
		Vec{X: 6, Y: 1.5, Z: 10},
		&Sphere{
			Radius:   2,
			Material: HitLetterYellow,
		},
	})
	scene.Ms = append(scene.Ms, &PositionedModel{
		Vec{X: -4, Y: 2.5, Z: 20},
		&Sphere{
			Radius:   3,
			Material: HitLetterRed,
		},
	})
	fmt.Fprintf(os.Stderr, "\n")
	return scene
}

// subPixelJitter takes the origin pixel and applies a bit of random noise jitter to it
// for for each sample.
// TODO(lostluck): investigate if I can reuse this for Depth of Field.
func subPixelJitter(x, y int, cfg ImageConfig) Vec {
	upJitter := cfg.Up.Times(MonoVec(float64(y) - cfg.Height/2 + RandomVal()))
	leftJitter := cfg.Left.Times(MonoVec(float64(x) - cfg.Width/2 + RandomVal()))
	return cfg.Goal.Plus(leftJitter).Plus(upJitter).InvSqrt()
}

// OutputPath generates a png file path based on the given directory, path and number of samples.
func OutputPath(dir, word string, samples int) string {
	return strings.Replace(path.Join(dir, fmt.Sprintf("%s.%d.png", word, samples)), ":/", "://", 1)
}
