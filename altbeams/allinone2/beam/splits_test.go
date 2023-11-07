package beam

import (
	"context"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/slog"
)

// TestSplitHelper tests the underlying split logic to confirm that various
// cases produce expected split points.
func TestSplitHelper(t *testing.T) {
	eps := 0.00001 // Float comparison precision.

	// Test splits at various fractions.
	t.Run("SimpleSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			frac       float64
			want       int64
		}{
			// Split as close to the beginning as possible.
			{curr: 0, size: 16, frac: 0, want: 1},
			// The closest split is at 4, even when just above or below it.
			{curr: 0, size: 16, frac: 0.24, want: 4},
			{curr: 0, size: 16, frac: 0.25, want: 4},
			{curr: 0, size: 16, frac: 0.26, want: 4},
			// Split the *remainder* in half.
			{curr: 0, size: 16, frac: 0.5, want: 8},
			{curr: 2, size: 16, frac: 0.5, want: 9},
			{curr: 6, size: 16, frac: 0.5, want: 11},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, test.curr, test.size), func(t *testing.T) {
				wantFrac := -1.0
				got, gotFrac, err := splitHelper(test.curr, test.size, 0.0, nil, test.frac, false)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, wantFrac)
				}
			})
		}
	})

	t.Run("WithElementProgress", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			currProg   float64
			frac       float64
			want       int64
		}{
			// Progress into the active element influences where the split of
			// the remainder falls.
			{curr: 0, currProg: 0.5, size: 4, frac: 0.25, want: 1},
			{curr: 0, currProg: 0.9, size: 4, frac: 0.25, want: 2},
			{curr: 1, currProg: 0.0, size: 4, frac: 0.25, want: 2},
			{curr: 1, currProg: 0.1, size: 4, frac: 0.25, want: 2},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, float64(test.curr)+test.currProg, test.size), func(t *testing.T) {
				wantFrac := -1.0
				got, gotFrac, err := splitHelper(test.curr, test.size, test.currProg, nil, test.frac, false)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, wantFrac)
				}
			})
		}
	})

	// Test splits with allowed split points.
	t.Run("WithAllowedSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			splits     []int64
			frac       float64
			want       int64
			err        bool // True if test should cause a failure.
		}{
			// The desired split point is at 4.
			{curr: 0, size: 16, splits: []int64{2, 3, 4, 5}, frac: 0.25, want: 4},
			// If we can't split at 4, choose the closest possible split point.
			{curr: 0, size: 16, splits: []int64{2, 3, 5}, frac: 0.25, want: 5},
			{curr: 0, size: 16, splits: []int64{2, 3, 6}, frac: 0.25, want: 3},
			// Also test the case where all possible split points lie above or
			// below the desired split point.
			{curr: 0, size: 16, splits: []int64{5, 6, 7}, frac: 0.25, want: 5},
			{curr: 0, size: 16, splits: []int64{1, 2, 3}, frac: 0.25, want: 3},
			// We have progressed beyond all possible split points, so can't split.
			{curr: 5, size: 16, splits: []int64{1, 2, 3}, frac: 0.25, err: true},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v], splits = %v)", test.frac, test.curr, test.size, test.splits), func(t *testing.T) {
				wantFrac := -1.0
				got, gotFrac, err := splitHelper(test.curr, test.size, 0.0, test.splits, test.frac, false)
				if test.err {
					if err == nil {
						t.Fatalf("splitHelper should have errored, instead got: %v", got)
					}
					return
				}
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, wantFrac)
				}
			})
		}
	})

	t.Run("SdfSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			currProg   float64
			frac       float64
			want       int64
			wantFrac   float64
		}{
			// Split between future elements at element boundaries.
			{curr: 0, currProg: 0, size: 4, frac: 0.51, want: 2, wantFrac: -1.0},
			{curr: 0, currProg: 0, size: 4, frac: 0.49, want: 2, wantFrac: -1.0},
			{curr: 0, currProg: 0, size: 4, frac: 0.26, want: 1, wantFrac: -1.0},
			{curr: 0, currProg: 0, size: 4, frac: 0.25, want: 1, wantFrac: -1.0},

			// If the split falls inside the first, splittable element, split there.
			{curr: 0, currProg: 0, size: 4, frac: 0.20, want: 0, wantFrac: 0.8},
			// The choice of split depends on the progress into the first element.
			{curr: 0, currProg: 0, size: 4, frac: 0.125, want: 0, wantFrac: 0.5},
			// Here we are far enough into the first element that splitting at 0.2 of the
			// remainder falls outside the first element.
			{curr: 0, currProg: 0.5, size: 4, frac: 0.2, want: 1, wantFrac: -1.0},

			// Verify the above logic when we are partially through the stream.
			{curr: 2, currProg: 0, size: 4, frac: 0.6, want: 3, wantFrac: -1.0},
			{curr: 2, currProg: 0.9, size: 4, frac: 0.6, want: 4, wantFrac: -1.0},
			{curr: 2, currProg: 0.5, size: 4, frac: 0.2, want: 2, wantFrac: 0.6},

			// Verify that the fraction returned is in terms of remaining work.
			{curr: 0, currProg: 0.8, size: 1, frac: 0.5, want: 0, wantFrac: 0.5},
			{curr: 0, currProg: 0.4, size: 2, frac: 0.1875, want: 0, wantFrac: 0.5},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, float64(test.curr)+test.currProg, test.size), func(t *testing.T) {
				got, gotFrac, err := splitHelper(test.curr, test.size, test.currProg, nil, test.frac, true)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, test.wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, test.wantFrac)
				}
			})
		}
	})

	t.Run("SdfWithAllowedSplits", func(t *testing.T) {
		tests := []struct {
			curr, size int64
			currProg   float64
			frac       float64
			splits     []int64
			want       int64
			wantFrac   float64
		}{
			// This is where we would like to split, when all split points are available.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 3, 4, 5}, want: 2, wantFrac: 0.6},
			// We can't split element at index 2, because 3 is not a split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 4, 5}, want: 4, wantFrac: -1.0},
			// We can't even split element at index 4 as above, because 4 is also not a
			// split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 2, 5}, want: 5, wantFrac: -1.0},
			// We can't split element at index 2, because 2 is not a split point.
			{curr: 2, currProg: 0, size: 5, frac: 0.2, splits: []int64{1, 3, 4, 5}, want: 3, wantFrac: -1.0},
		}
		for _, test := range tests {
			test := test
			t.Run(fmt.Sprintf("(%v of [%v, %v])", test.frac, float64(test.curr)+test.currProg, test.size), func(t *testing.T) {
				got, gotFrac, err := splitHelper(test.curr, test.size, test.currProg, test.splits, test.frac, true)
				if err != nil {
					t.Fatalf("error in splitHelper: %v", err)
				}
				if got != test.want {
					t.Errorf("incorrect split point: got: %v, want: %v", got, test.want)
				}
				if !floatEquals(gotFrac, test.wantFrac, eps) {
					t.Errorf("incorrect split fraction: got: %v, want: %v", gotFrac, test.wantFrac)
				}
			})
		}
	})
}

func floatEquals(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

// TODO, add separation harness test?

// Global variable, so only one is registered with the OS.
var ws = &Watchers{}

// Watcher is an instance of the counters.
type watcher struct {
	id                         int
	mu                         sync.Mutex
	sentinelCount, sentinelCap int
}

func (w *watcher) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int("id", w.id),
		slog.Int("sentinelCount", w.sentinelCount),
		slog.Int("sentinelCap", w.sentinelCap),
	)
}

// Watchers is a "net/rpc" service.
type Watchers struct {
	mu             sync.Mutex
	nextID         int
	lookup         map[int]*watcher
	serviceOnce    sync.Once
	serviceAddress string
}

// Args is the set of parameters to the watchers RPC methods.
type Args struct {
	WatcherID int
}

// Block is called once per sentinel, to indicate it will block
// until all sentinels are blocked.
func (ws *Watchers) Block(args *Args, _ *bool) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.lookup[args.WatcherID]
	if !ok {
		return fmt.Errorf("no watcher with id %v", args.WatcherID)
	}
	w.mu.Lock()
	w.sentinelCount++
	slog.Debug("sentinel target increased", slog.Int("watcherID", args.WatcherID), slog.Int("count", w.sentinelCount))
	w.mu.Unlock()
	return nil
}

// Check returns whether the sentinels are unblocked or not.
func (ws *Watchers) Check(args *Args, unblocked *bool) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.lookup[args.WatcherID]
	if !ok {
		return fmt.Errorf("no watcher with id %v", args.WatcherID)
	}
	w.mu.Lock()
	*unblocked = w.sentinelCount >= w.sentinelCap
	slog.Debug("sentinel target for watcher not met", slog.Int("watcherID", args.WatcherID), slog.Int("count", w.sentinelCount), slog.Int("target", w.sentinelCap))
	w.mu.Unlock()
	return nil
}

// Delay returns whether the sentinels should delay.
// This increments the sentinel cap, and returns unblocked.
// Intended to validate ProcessContinuation behavior.
func (ws *Watchers) Delay(args *Args, delay *bool) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	w, ok := ws.lookup[args.WatcherID]
	if !ok {
		return fmt.Errorf("no watcher with id %v", args.WatcherID)
	}
	w.mu.Lock()
	w.sentinelCount++
	// Delay as long as the sentinel count is under the cap.
	*delay = w.sentinelCount < w.sentinelCap
	w.mu.Unlock()
	slog.Debug("Delay: sentinel target", "watcher", w, slog.Bool("delay", *delay))
	return nil
}

func (ws *Watchers) initRPCServer() {
	ws.serviceOnce.Do(func() {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(err)
		}
		rpc.Register(ws)
		rpc.HandleHTTP()
		go http.Serve(l, nil)
		ws.serviceAddress = l.Addr().String()
	})
}

// newWatcher starts an rpc server to manage state for watching for
// sentinels across local machines.
func (ws *Watchers) newWatcher(sentinelCap int) int {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.initRPCServer()
	if ws.lookup == nil {
		ws.lookup = map[int]*watcher{}
	}
	w := &watcher{id: ws.nextID, sentinelCap: sentinelCap}
	ws.nextID++
	ws.lookup[w.id] = w
	return w.id
}

// sepHarnessBase contains fields and functions that are shared by all
// versions of the separation harness.
type sepHarnessBase[E comparable] struct {
	WatcherID    int
	Sleep        time.Duration
	Sentinels    []E
	LocalService string
}

// One connection per binary.
var (
	sepClientOnce sync.Once
	sepClient     *rpc.Client
	sepClientMu   sync.Mutex
	sepWaitMap    map[int]chan struct{}
)

func (fn *sepHarnessBase[E]) setup() error {
	sepClientMu.Lock()
	defer sepClientMu.Unlock()
	sepClientOnce.Do(func() {
		client, err := rpc.DialHTTP("tcp", fn.LocalService)
		if err != nil {
			slog.Error("failed to dial sentinels  server", err, slog.String("endpoint", fn.LocalService))
			panic(fmt.Sprintf("dialing sentinels server %v: %v", fn.LocalService, err))
		}
		sepClient = client
		sepWaitMap = map[int]chan struct{}{}
	})

	// Check if there's already a local channel for this id, and if not
	// start a watcher goroutine to poll and unblock the harness when
	// the expected number of sentinels is reached.
	if _, ok := sepWaitMap[fn.WatcherID]; ok {
		return nil
	}
	// We need a channel to block on for this watcherID
	// We use a channel instead of a wait group since the finished
	// count is hosted in a different process.
	c := make(chan struct{})
	sepWaitMap[fn.WatcherID] = c
	go func(id int, c chan struct{}) {
		for {
			time.Sleep(time.Millisecond * 50) // Check counts every 50 milliseconds.
			sepClientMu.Lock()
			var unblock bool
			err := sepClient.Call("Watchers.Check", &Args{WatcherID: id}, &unblock)
			if err != nil {
				slog.Error("Watchers.Check: sentinels server error", err, slog.String("endpoint", fn.LocalService))
				panic("sentinel server error")
			}
			if unblock {
				close(c) // unblock all the local waiters.
				slog.Debug("sentinel target for watcher, unblocking", slog.Int("watcherID", id))
				sepClientMu.Unlock()
				return
			}
			slog.Debug("sentinel target for watcher not met", slog.Int("watcherID", id))
			sepClientMu.Unlock()
		}
	}(fn.WatcherID, c)
	return nil
}

func (fn *sepHarnessBase[E]) isSentinel(elm E) bool {
	for _, v := range fn.Sentinels {
		if elm == v {
			return true
		}
	}
	return false
}

func (fn *sepHarnessBase[E]) block() {
	sepClientMu.Lock()
	var ignored bool
	err := sepClient.Call("Watchers.Block", &Args{WatcherID: fn.WatcherID}, &ignored)
	if err != nil {
		slog.Error("Watchers.Block error", err, slog.String("endpoint", fn.LocalService))
		panic(err)
	}
	c := sepWaitMap[fn.WatcherID]
	sepClientMu.Unlock()

	// Block until the watcher closes the channel.
	<-c
}

// delay inform the DoFn whether or not to return a delayed Processing continuation for this position.
func (fn *sepHarnessBase[E]) delay() bool {
	sepClientMu.Lock()
	defer sepClientMu.Unlock()
	var delay bool
	err := sepClient.Call("Watchers.Delay", &Args{WatcherID: fn.WatcherID}, &delay)
	if err != nil {
		slog.Error("Watchers.Delay error", err)
		panic(err)
	}
	return delay
}

// sepHarness is a simple DoFn that blocks when reaching a sentinel.
// It's useful for testing blocks on channel splits.
type sepHarness[E comparable] struct {
	Base sepHarnessBase[E]

	Output Output[E]
}

func (fn *sepHarness[E]) ProcessBundle(ctx context.Context, dfc *DFC[E]) error {
	fn.Base.setup()

	return dfc.Process(func(ec ElmC, elm E) error {
		if fn.Base.isSentinel(elm) {
			fn.Base.block()
		} else {
			time.Sleep(fn.Base.Sleep)
		}
		fn.Output.Emit(ec, elm)
		return nil
	})
}

func TestSeparation(t *testing.T) {
	ws.initRPCServer()

	tests := []struct {
		name     string
		pipeline func(s *Scope) error
	}{
		{
			name: "ChannelSplit",
			pipeline: func(s *Scope) error {
				imp := Impulse(s)
				src := ParDo(s, imp, &SourceFn{Count: 10})
				sep := ParDo(s, Reshuffle(s, src.Output), &sepHarness[int]{
					Base: sepHarnessBase[int]{
						WatcherID:    ws.newWatcher(3),
						Sleep:        10 * time.Millisecond,
						Sentinels:    []int{2, 5, 8},
						LocalService: ws.serviceAddress,
					},
				})
				ParDo(s, sep.Output, &DiscardFn[int]{}, Name("sink"))
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pr, err := Run(context.Background(), test.pipeline)
			if err != nil {
				t.Error(err)
			}
			if got, want := int(pr.Counters["sink.Processed"]), 10; got != want {
				t.Fatalf("processed didn't match bench number: got %v want %v", got, want)
			}
		})
	}
}
