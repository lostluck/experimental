package harness

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	fnpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/fnexecution_v1"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ExecFunc func(context.Context, *Control, DataContext) (*fnpb.ProcessBundleResponse, error)

// Options for harness.Main that affect execution of the harness, such as runner capabilities.
type Options struct {
	RunnerCapabilities []string // URNs for what runners are able to understand over the FnAPI.
	LoggingEndpoint    string   // Endpoint for remote logging.
	StatusEndpoint     string   // Endpoint for worker status reporting.
}

func Main(ctx context.Context, controlEndpoint string, opts Options, exec ExecFunc) error {
	// Connect to FnAPI control server. Receive and execute work.
	conn, err := Dial(ctx, controlEndpoint, 60*time.Second)
	if err != nil {
		return errors.Wrap(err, "failed to connect")
	}
	defer conn.Close()

	client := fnpb.NewBeamFnControlClient(conn)

	controlStub, err := client.Control(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to connect to control service")
	}
	var wg sync.WaitGroup
	respc := make(chan *fnpb.InstructionResponse, 100)

	wg.Add(1)

	// gRPC requires all writers to a stream be the same goroutine, so this is the
	// goroutine for managing responses back to the control service.
	go func() {
		defer wg.Done()
		for resp := range respc {
			if err := controlStub.Send(resp); err != nil {
				slog.ErrorContext(ctx, "control.Send: Failed to respond", "error", err)
			}
		}
		slog.DebugContext(ctx, "control response channel closed")
	}()

	ctrl := &Control{
		dataMan:     &DataChannelManager{},
		stateMan:    &StateChannelManager{},
		descriptors: map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor{},
		plans:       map[bundleDescriptorID][]any{},
		monitors:    map[instructionID]Monitor{},

		exec: exec,
		fetchBD: func(id bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error) {
			return client.GetProcessBundleDescriptor(ctx, &fnpb.GetProcessBundleDescriptorRequest{ProcessBundleDescriptorId: string(id)})
		},
	}
	// gRPC requires all readers of a stream be the same goroutine, so this goroutine
	// is responsible for managing the network data. All it does is pull data from
	// the stream, and hand off the message to a goroutine to actually be handled,
	// so as to avoid blocking the underlying network channel.
	var shutdown int32
	for {
		req, err := controlStub.Recv()
		if err != nil {
			// An error means we can't send or receive anymore. Shut down.
			atomic.AddInt32(&shutdown, 1)
			close(respc)
			wg.Wait()
			if err == io.EOF {
				return nil
			}
			return errors.Wrapf(err, "control.Recv failed")
		}

		// Launch a goroutine to handle the control message.
		fn := func(ctx context.Context, req *fnpb.InstructionRequest) {
			resp := handleInstruction(ctx, req, ctrl)

			if resp != nil && atomic.LoadInt32(&shutdown) == 0 {
				respc <- resp
			}
		}

		if req.GetProcessBundle() != nil {
			// Add this to the inactive queue before allowing other requests
			// to be processed. This prevents race conditions with split
			// or progress requests for this instruction.
			// ctrl.mu.Lock()
			// ctrl.inactive.Add(instructionID(req.GetInstructionId()))
			// ctrl.mu.Unlock()
			// Only process bundles in a goroutine. We at least need to process instructions for
			// each plan serially. Perhaps just invoke plan.Execute async?
			go fn(ctx, req)
		} else {
			fn(ctx, req)
		}
	}
}

// Dial is a convenience wrapper over grpc.Dial. It can be overridden
// to provide a customized dialing behavior.
var Dial = DefaultDial

// DefaultDial is a dialer that specifies an insecure blocking connection with a timeout.
func DefaultDial(ctx context.Context, endpoint string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cc, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)))
	if err != nil {
		return nil, fmt.Errorf("failed to dial server at %v: %w", endpoint, err)
	}
	return cc, nil
}

var (
	labelMu      sync.Mutex
	cachedLabels = map[string]*pipepb.MonitoringInfo{}
)

func handleInstruction(ctx context.Context, req *fnpb.InstructionRequest, ctrl *Control) *fnpb.InstructionResponse {
	instID := instructionID(req.GetInstructionId())
	//ctx = metrics.SetBundleID(ctx, string(instID))

	switch {
	// case req.GetRegister() != nil:
	// 	//	msg := req.GetRegister()

	// 	//	c.mu.Lock()
	// 	//	for _, desc := range msg.GetProcessBundleDescriptor() {
	// 	//		c.descriptors[bundleDescriptorID(desc.GetId())] = desc
	// 	//	}
	// 	//	c.mu.Unlock()

	// 	return &fnpb.InstructionResponse{
	// 		InstructionId: string(instID),
	// 		Response: &fnpb.InstructionResponse_Register{
	// 			Register: &fnpb.RegisterResponse{},
	// 		},
	// 	}

	case req.GetProcessBundle() != nil:
		msg := req.GetProcessBundle()

		bdID := bundleDescriptorID(msg.GetProcessBundleDescriptorId())

		data := NewScopedDataManager(ctrl.dataMan, instID)
		state := NewScopedStateManager(ctrl.stateMan, instID)

		pbr, err := ctrl.exec(ctx, ctrl, DataContext{Data: data, State: state, bdID: bdID, instID: instID})
		if err != nil {
			return fail(ctx, instID, "process bundle failed %v", err)
		}

		ctrl.mu.Lock()
		mon, ok := ctrl.monitors[instID]
		ctrl.mu.Unlock()
		if ok {
			labels, pylds := mon()

			labelMu.Lock()
			maps.Copy(cachedLabels, labels)
			labelMu.Unlock()

			pbr.MonitoringData = pylds
		}

		// TODO(lostluck): 2023/03/29 fix debug level logging to be flagged.
		// log.Debugf(ctx, "PB [%v]: %v", instID, msg)
		//plan, err := c.getOrCreatePlan(bdID)

		// Make the plan active.
		// c.mu.Lock()
		// c.inactive.Remove(instID)
		// c.active[instID] = plan
		// // Get the user metrics store for this bundle.
		// store := metrics.GetStore(ctx)
		// c.metStore[instID] = store
		// c.mu.Unlock()

		// if err != nil {
		// 	c.failed[instID] = err
		// 	return fail(ctx, instID, "ProcessBundle failed: %v", err)
		// }

		// tokens := msg.GetCacheTokens()
		// c.cache.SetValidTokens(tokens...)

		// // data := NewScopedDataManager(c.data, instID)
		// // state := NewScopedStateReaderWithCache(c.state, instID, c.cache)

		// // sampler := newSampler(store)
		// // go sampler.start(ctx, samplePeriod)

		// // err = plan.Execute(ctx, string(instID), exec.DataContext{Data: data, State: state})

		// // sampler.stop()

		// // dataError := data.Close()
		// // state.Close()

		// // c.cache.CompleteBundle(tokens...)

		// //	mons, pylds := monitoring(plan, store, c.runnerCapabilities[URNMonitoringInfoShortID])

		// checkpoints := plan.Checkpoint()
		// requiresFinalization := false
		// // Move the plan back to the candidate state
		// c.mu.Lock()
		// // Mark the instruction as failed.
		// if err != nil {
		// 	c.failed[instID] = err
		// } else if dataError != io.EOF && dataError != nil {
		// 	// If there was an error on the data channel reads, fail this bundle
		// 	// since we may have had a short read.
		// 	c.failed[instID] = dataError
		// 	err = dataError
		// } else {
		// 	// Non failure plans should either be moved to the finalized state
		// 	// or to plans so they can be re-used.
		// 	expiration := plan.GetExpirationTime()
		// 	if time.Now().Before(expiration) {
		// 		// TODO(BEAM-10976) - we can be a little smarter about data structures here by
		// 		// by storing plans awaiting finalization in a heap. That way when we expire plans
		// 		// here its O(1) instead of O(n) (though adding/finalizing will still be O(logn))
		// 		requiresFinalization = true
		// 		// c.awaitingFinalization[instID] = awaitingFinalization{
		// 		// 	expiration: expiration,
		// 		// 	plan:       plan,
		// 		// 	bdID:       bdID,
		// 		// }
		// 		// Move any plans that have exceeded their expiration back into the re-use pool
		// 		for id, af := range c.awaitingFinalization {
		// 			if time.Now().After(af.expiration) {
		// 				c.plans[af.bdID] = append(c.plans[af.bdID], af.plan)
		// 				delete(c.awaitingFinalization, id)
		// 			}
		// 		}
		// 	} else {
		// 		c.plans[bdID] = append(c.plans[bdID], plan)
		// 	}
		// }

		// var rRoots []*fnpb.DelayedBundleApplication
		// if len(checkpoints) > 0 {
		// 	for _, cp := range checkpoints {
		// 		for _, r := range cp.SR.RS {
		// 			rRoots = append(rRoots, &fnpb.DelayedBundleApplication{
		// 				Application: &fnpb.BundleApplication{
		// 					TransformId:      cp.SR.TId,
		// 					InputId:          cp.SR.InId,
		// 					Element:          r,
		// 					OutputWatermarks: cp.SR.OW,
		// 				},
		// 				RequestedTimeDelay: durationpb.New(cp.Reapply),
		// 			})
		// 		}
		// 	}
		// }

		// delete(c.active, instID)
		// if removed, ok := c.inactive.Insert(instID); ok {
		// 	delete(c.failed, removed) // Also GC old failed bundles.
		// }
		// delete(c.metStore, instID)

		// c.mu.Unlock()

		// if err != nil {
		// 	return fail(ctx, instID, "process bundle failed for instruction %v using plan %v : %v", instID, bdID, err)
		// }
		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundle{
				ProcessBundle: pbr,
			},
		}

	case req.GetFinalizeBundle() != nil:
		// msg := req.GetFinalizeBundle()

		// ref := instructionID(msg.GetInstructionId())

		// af, ok := c.awaitingFinalization[ref]
		// if !ok {
		// 	return fail(ctx, instID, "finalize bundle failed for instruction %v: couldn't find plan in finalizing map", ref)
		// }

		// if time.Now().Before(af.expiration) {
		// 	if err := af.plan.Finalize(); err != nil {
		// 		return fail(ctx, instID, "finalize bundle failed for instruction %v using plan %v : %v", ref, af.bdID, err)
		// 	}
		// }
		// c.plans[af.bdID] = append(c.plans[af.bdID], af.plan)
		// delete(c.awaitingFinalization, ref)

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_FinalizeBundle{
				FinalizeBundle: &fnpb.FinalizeBundleResponse{},
			},
		}

	case req.GetProcessBundleProgress() != nil:
		msg := req.GetProcessBundleProgress()

		instID := instructionID(msg.GetInstructionId())
		ctrl.mu.Lock()
		mon := ctrl.monitors[instID]
		ctrl.mu.Unlock()

		labels, pylds := mon()

		labelMu.Lock()
		maps.Copy(cachedLabels, labels)
		labelMu.Unlock()

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleProgress{
				ProcessBundleProgress: &fnpb.ProcessBundleProgressResponse{
					MonitoringData: pylds,
				},
			},
		}

	case req.GetProcessBundleSplit() != nil:
		// msg := req.GetProcessBundleSplit()

		// // TODO(lostluck): 2023/03/29 fix debug level logging to be flagged.
		// // log.Debugf(ctx, "PB Split: %v", msg)
		// ref := instructionID(msg.GetInstructionId())

		// plan, _, resp := c.getPlanOrResponse(ctx, "split", instID, ref)
		// if resp != nil {
		// 	return resp
		// }
		// if plan == nil {
		// 	return &fnpb.InstructionResponse{
		// 		InstructionId: string(instID),
		// 		Response: &fnpb.InstructionResponse_ProcessBundleSplit{
		// 			ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{},
		// 		},
		// 	}
		// }

		// // Get the desired splits for the root FnAPI read operation.
		// ds := msg.GetDesiredSplits()[plan.SourcePTransformID()]
		// if ds == nil {
		// 	return fail(ctx, instID, "failed to split: desired splits for root of %v was empty.", ref)
		// }
		// sr, err := plan.Split(ctx, exec.SplitPoints{
		// 	Splits:  ds.GetAllowedSplitPoints(),
		// 	Frac:    ds.GetFractionOfRemainder(),
		// 	BufSize: ds.GetEstimatedInputElements(),
		// })

		// if err != nil {
		// 	return fail(ctx, instID, "unable to split %v: %v", ref, err)
		// }

		// // Unsuccessful splits without errors indicate we should return an empty response,
		// // as processing can continue.
		// if sr.Unsuccessful {
		// 	return &fnpb.InstructionResponse{
		// 		InstructionId: string(instID),
		// 		Response: &fnpb.InstructionResponse_ProcessBundleSplit{
		// 			ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{},
		// 		},
		// 	}
		// }

		// var pRoots []*fnpb.BundleApplication
		// var rRoots []*fnpb.DelayedBundleApplication
		// if sr.PS != nil && len(sr.PS) > 0 && sr.RS != nil && len(sr.RS) > 0 {
		// 	pRoots = make([]*fnpb.BundleApplication, len(sr.PS))
		// 	for i, p := range sr.PS {
		// 		pRoots[i] = &fnpb.BundleApplication{
		// 			TransformId: sr.TId,
		// 			InputId:     sr.InId,
		// 			Element:     p,
		// 		}
		// 	}
		// 	rRoots = make([]*fnpb.DelayedBundleApplication, len(sr.RS))
		// 	for i, r := range sr.RS {
		// 		rRoots[i] = &fnpb.DelayedBundleApplication{
		// 			Application: &fnpb.BundleApplication{
		// 				TransformId:      sr.TId,
		// 				InputId:          sr.InId,
		// 				Element:          r,
		// 				OutputWatermarks: sr.OW,
		// 			},
		// 		}
		// 	}
		// }

		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_ProcessBundleSplit{
				ProcessBundleSplit: &fnpb.ProcessBundleSplitResponse{
					// ChannelSplits: []*fnpb.ProcessBundleSplitResponse_ChannelSplit{{
					// 	TransformId:          plan.SourcePTransformID(),
					// 	LastPrimaryElement:   sr.PI,
					// 	FirstResidualElement: sr.RI,
					// }},
					// PrimaryRoots:  pRoots,
					// ResidualRoots: rRoots,
				},
			},
		}
	case req.GetMonitoringInfos() != nil:
		needs := req.GetMonitoringInfos().GetMonitoringInfoId()
		labels := make(map[string]*pipepb.MonitoringInfo, len(needs))
		labelMu.Lock()
		for _, v := range needs {
			labels[v] = cachedLabels[v]
		}
		labelMu.Unlock()
		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_MonitoringInfos{
				MonitoringInfos: &fnpb.MonitoringInfosMetadataResponse{
					MonitoringInfo: labels,
				},
			},
		}
	case req.GetHarnessMonitoringInfos() != nil:
		return &fnpb.InstructionResponse{
			InstructionId: string(instID),
			Response: &fnpb.InstructionResponse_HarnessMonitoringInfos{
				HarnessMonitoringInfos: &fnpb.HarnessMonitoringInfosResponse{
					// TODO(BEAM-11092): Populate with non-bundle metrics data.
					MonitoringData: map[string][]byte{},
				},
			},
		}

	default:
		return fail(ctx, instID, "Unexpected request: %v", req)
	}
}

type bundleDescriptorID string
type instructionID string

func fail(ctx context.Context, id instructionID, format string, args ...any) *fnpb.InstructionResponse {
	slog.ErrorContext(ctx, fmt.Sprintf(format, args...))
	dummy := &fnpb.InstructionResponse_Register{Register: &fnpb.RegisterResponse{}}

	return &fnpb.InstructionResponse{
		InstructionId: string(id),
		Error:         fmt.Sprintf(format, args...),
		Response:      dummy,
	}
}

type Control struct {
	fetchBD  func(bundleDescriptorID) (*fnpb.ProcessBundleDescriptor, error)
	exec     ExecFunc
	dataMan  *DataChannelManager
	stateMan *StateChannelManager

	mu          sync.Mutex
	plans       map[bundleDescriptorID][]any
	singleDesc  singleflight.Group
	descriptors map[bundleDescriptorID]*fnpb.ProcessBundleDescriptor
	monitors    map[instructionID]Monitor
}

// GetOrLookupPlan does a layered check to get an execution plan.
//
// If there's a cached plan available already.  If so, we're done.
// Otherwise we'll need to build a new one from a ProcessBundleDescriptor.
// We first check the local cache, and if it doesn't exist, we request it
// from the runner, reducing duplicate requests.
func (ctrl *Control) GetOrLookupPlan(dc DataContext, unmarshal func(pbd *fnpb.ProcessBundleDescriptor) any) (any, error) {
	ctrl.mu.Lock()
	if p, ok := ctrl.plans[dc.bdID]; ok {
		ctrl.mu.Unlock()
		return p, nil
	}
	pbd, ok := ctrl.descriptors[dc.bdID]
	ctrl.mu.Unlock()
	if !ok {
		desc, err, _ := ctrl.singleDesc.Do(string(dc.bdID), func() (any, error) {
			pbd, err := ctrl.fetchBD(dc.bdID)
			if err != nil {
				return nil, err
			}
			ctrl.mu.Lock()
			ctrl.descriptors[dc.bdID] = pbd
			ctrl.mu.Unlock()
			return pbd, nil
		})
		if err != nil {
			return err, nil
		}
		pbd = desc.(*fnpb.ProcessBundleDescriptor)
	}

	return unmarshal(pbd), nil
}

// Monitor is a function that returns any new labels, and the set of payloads
// being returned to the runner.
type Monitor func() (map[string]*pipepb.MonitoringInfo, map[string][]byte)

func (ctrl *Control) RegisterMonitor(dc DataContext, monFn Monitor) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()
	ctrl.monitors[dc.instID] = monFn
}
