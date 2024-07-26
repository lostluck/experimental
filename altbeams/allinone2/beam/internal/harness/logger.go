package harness

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"time"

	"github.com/jba/slog/withsupport"
	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Produces a logger that has a handler attached to the Beam Logging Service.
// Built by following the guide:
// https://github.com/golang/example/blob/master/slog-handler-guide/README.md

type loggingHandler struct {
	opts handlerOptions

	with *withsupport.GroupOrAttrs
	out  chan *fnpb.LogEntry
}

func (h *loggingHandler) WithGroup(name string) slog.Handler {
	return &loggingHandler{h.opts, h.with.WithGroup(name), h.out}
}

// TransformID will be provided to cached loggers under the hood.
// So we only need to look for it in WithAttrs, and not when handling
// a record.
const transformIDKey = "beamTransformID"

func withTransformID(tid string) slog.Attr {
	return slog.Attr{Key: transformIDKey, Value: slog.StringValue(tid)}
}

func (h *loggingHandler) WithAttrs(as []slog.Attr) slog.Handler {
	newOpts := h.opts
	for i, a := range as {
		if a.Key == transformIDKey {
			newOpts.TransformId = a.Value.String()
			as = slices.Delete(as, i, i+1)
			break
		}
	}
	return &loggingHandler{newOpts, h.with.WithAttrs(as), h.out}
}

type handlerOptions struct {
	// Level reports the minimum level to log.
	// Levels with lower levels are discarded.
	// If nil, the Handler uses [slog.LevelInfo].
	Level       slog.Leveler
	// TODO make this like the slog.Leveler so we can change it in re-used bundle plans.
	InstID      instructionID 
	TransformId string // edge id.
}

func newLoggingHandler(out chan *fnpb.LogEntry, opts *handlerOptions) *loggingHandler {
	h := &loggingHandler{
		out: out,
	}
	if opts != nil {
		h.opts = *opts
	}
	if h.opts.Level == nil {
		h.opts.Level = slog.LevelInfo
	}
	return h
}

func (h *loggingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.opts.Level.Level()
}

func (h *loggingHandler) Handle(ctx context.Context, r slog.Record) error {
	entry := &fnpb.LogEntry{
		InstructionId: string(h.opts.InstID),
		TransformId:   string(h.opts.TransformId),
		Severity:      messageSeverity(r.Level),
		Message:       r.Message,
	}
	if !r.Time.IsZero() {
		entry.Timestamp = timestamppb.New(r.Time)
	}
	if r.PC != 0 {
		fs := runtime.CallersFrames([]uintptr{r.PC})
		f, _ := fs.Next()
		entry.LogLocation = fmt.Sprintf("%s:%d", f.File, f.Line)
	}

	if r.NumAttrs() > 0 || (h.with != nil) {
		entry.CustomData = &structpb.Struct{
			Fields: map[string]*structpb.Value{},
		}
	}

	groups := h.with.Apply(func(groups []string, a slog.Attr) {
		h.addAttr(groups, entry.GetCustomData(), a)
	})
	r.Attrs(func(a slog.Attr) bool {
		h.addAttr(groups, entry.GetCustomData(), a)
		return true
	})
	h.out <- entry
	return nil
}

func (h *loggingHandler) addAttr(groups []string, data *structpb.Struct, a slog.Attr) {
	// Resolve the Attr's value before doing anything else.
	a.Value = a.Value.Resolve()
	// Ignore empty Attrs.
	if a.Equal(slog.Attr{}) {
		return
	}
	cur := data
	for _, g := range groups {
		next := cur.Fields[g].GetStructValue()
		if next == nil {
			next = &structpb.Struct{
				Fields: map[string]*structpb.Value{},
			}
			cur.Fields[g] = structpb.NewStructValue(next)
		}
		cur = next
	}
	if sv := slogValue2StructValue(a.Value); sv != nil {
		if a.Value.Kind() == slog.KindGroup && a.Key == "" {
			// Groups with empty keys are inlined into their parents.
			for k, v := range sv.GetStructValue().Fields {
				cur.Fields[k] = v
			}
		} else {
			cur.Fields[a.Key] = sv
		}
	}
	return
}

func slogValue2StructValue(v slog.Value) *structpb.Value {
	switch v.Kind() {
	case slog.KindTime:
		// Write times in a standard way, without the monotonic time.
		return structpb.NewStringValue(v.Time().Format(time.RFC3339Nano))
	case slog.KindGroup:
		attrs := v.Group()
		// Ignore empty groups.
		if len(attrs) == 0 {
			return nil
		}
		// If the key is non-empty, write it out and indent the rest of the attrs.
		// Otherwise, inline the attrs.
		fields := &structpb.Struct{
			Fields: map[string]*structpb.Value{},
		}
		for _, ga := range attrs {
			if gv := slogValue2StructValue(ga.Value); gv != nil {
				fields.Fields[ga.Key] = gv
			}
		}
		return structpb.NewStructValue(fields)
	case slog.KindLogValuer:
		return slogValue2StructValue(v.LogValuer().LogValue())
	default:
		return structpb.NewStringValue(v.String())
	}
}

// Translate between standard slog levels and Beam logging levels.
// If the slog level has no direct comparison it's translated to the nearest lower band,
// with levels below  slog.Debug become trace level, and above slog.Error are critical.
func messageSeverity(severity slog.Level) fnpb.LogEntry_Severity_Enum {
	switch severity {
	case slog.LevelError:
		return fnpb.LogEntry_Severity_ERROR
	case slog.LevelWarn:
		return fnpb.LogEntry_Severity_WARN
	case slog.LevelInfo:
		return fnpb.LogEntry_Severity_INFO
	case slog.LevelDebug:
		return fnpb.LogEntry_Severity_DEBUG
	default:
		if severity < slog.LevelDebug {
			return fnpb.LogEntry_Severity_TRACE
		} else if severity < slog.LevelInfo {
			return fnpb.LogEntry_Severity_DEBUG
		} else if severity < slog.LevelWarn {
			return fnpb.LogEntry_Severity_INFO
		} else if severity < slog.LevelError {
			return fnpb.LogEntry_Severity_NOTICE
		}
		return fnpb.LogEntry_Severity_CRITICAL
	}
}
