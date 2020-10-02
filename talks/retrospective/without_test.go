package retrospective

import (
	"reflect"
	"testing"
)

func TestPipeline(t *testing.T) {
	fns := []Fn{Foo2, Bar2, Baz2}
	ifaceFns := []interface{}{Foo, Bar, Baz}
	var rFns []reflect.Value
	for _, iface := range ifaceFns {
		rFns = append(rFns, reflect.ValueOf(iface))
	}
	var errs []error
	var cFns []Caller
	for _, iface := range ifaceFns {
		c, err := MakeCaller(iface)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cFns = append(cFns, c)
	}
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	tests := []struct {
		name string
		pipe func(interface{}) interface{}
	}{
		{
			name: "directWassert",
			pipe: func(a interface{}) interface{} {
				return direct(a.(float64))
			},
		}, {
			name: "indirect",
			pipe: func(a interface{}) interface{} {
				return indirect(a)
			},
		}, {
			name: "indirectFuncs",
			pipe: indirectFuncs,
		}, {
			name: "pipeline",
			pipe: pipeline,
		}, {
			name: "pipelineOpt",
			pipe: func(a interface{}) interface{} {
				return pipelineOpt(a, fns)
			},
		}, {
			name: "reflectPipeline",
			pipe: reflectPipeline,
		}, {
			name: "reflectPipelineAdjusted",
			pipe: reflectPipelineAdjusted,
		}, {
			name: "reflectPipelineOpt",
			pipe: func(a interface{}) interface{} {
				return reflectPipelineOpt(a, rFns)
			},
		}, {
			name: "switchedPipeline",
			pipe: switchedPipeline,
		}, {
			name: "callerPipeline",
			pipe: callerPipeline,
		}, {
			name: "callerPipelineOpt",
			pipe: func(a interface{}) interface{} {
				return callerPipelineOpt(a, cFns)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, want := test.pipe(float64(input)).(string), "42"; got != want {
				t.Fatalf("got %v, want %v", got, want)
			}

		})
	}
}

func BenchmarkPipeline(b *testing.B) {
	fns := []Fn{Foo2, Bar2, Baz2}
	ifaceFns := []interface{}{Foo, Bar, Baz}
	var rFns []reflect.Value
	for _, iface := range ifaceFns {
		rFns = append(rFns, reflect.ValueOf(iface))
	}
	var errs []error
	var cFns []Caller
	for _, iface := range ifaceFns {
		c, err := MakeCaller(iface)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cFns = append(cFns, c)
	}
	if len(errs) != 0 {
		b.Fatal(errs)
	}
	benches := []struct {
		name string
		pipe func(interface{}) interface{}
	}{
		{
			name: "directWassert",
			pipe: func(a interface{}) interface{} {
				return direct(a.(float64))
			},
		}, {
			name: "indirect",
			pipe: func(a interface{}) interface{} {
				return indirect(a)
			},
		}, {
			name: "indirectFuncs",
			pipe: indirectFuncs,
		}, {
			name: "pipeline",
			pipe: pipeline,
		}, {
			name: "pipelineOpt",
			pipe: func(a interface{}) interface{} {
				return pipelineOpt(a, fns)
			},
		}, {
			name: "reflectPipeline",
			pipe: reflectPipeline,
		}, {
			name: "reflectPipelineAdjusted",
			pipe: reflectPipelineAdjusted,
		}, {
			name: "reflectPipelineOpt",
			pipe: func(a interface{}) interface{} {
				return reflectPipelineOpt(a, rFns)
			},
		}, {
			name: "switchedPipeline",
			pipe: switchedPipeline,
		}, {
			name: "callerPipeline",
			pipe: callerPipeline,
		}, {
			name: "callerPipelineOpt",
			pipe: func(a interface{}) interface{} {
				return callerPipelineOpt(a, cFns)
			},
		},
	}
	var s string
	var f float64
	b.Run("direct", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			f = float64(i)
			s = direct(f)
		}
	})
	b.Log(s)

	var a interface{}
	for _, bench := range benches {
		b.Run(bench.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				a = bench.pipe(float64(i))
			}
		})
	}
	b.Log(a)
}

func BenchmarkGenerate(b *testing.B) {
	ifaceFns := []interface{}{Foo, Bar, Baz}
	var errs []error
	var cFns []Caller
	for _, iface := range ifaceFns {
		c, err := MakeCaller(iface)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		cFns = append(cFns, c)
	}
	if len(errs) != 0 {
		b.Fatal(errs)
	}
	var out interface{}
	b.Run("generated", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			out = callerPipelineOpt(float64(i), cFns)
		}
	})
	b.Log(out)
}
