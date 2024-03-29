package universal

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/beamopts"
	"github.com/lostluck/experimental/altbeams/allinone2/beam/internal/harness"
	jobpb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/jobmanagement_v1"
	pipepb "github.com/lostluck/experimental/altbeams/allinone2/beam/internal/model/pipeline_v1"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/encoding/protowire"
)

var unique int32

// GetJobName returns the specified job name or, if not present, a fresh
// autogenerated name. Convenience function.
func getJobName(opts beamopts.Struct) string {
	if opts.Name == "" {
		id := atomic.AddInt32(&unique, 1)
		return fmt.Sprintf("go-job-%v-%v", id, time.Now().UnixNano())
	}
	return opts.Name
}

func Execute(ctx context.Context, p *pipepb.Pipeline, opts beamopts.Struct) (*Pipeline, error) {
	cc, err := harness.Dial(ctx, opts.Endpoint, 2*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("connecting to job service: %w", err)
	}
	client := jobpb.NewJobServiceClient(cc)

	prepReq := &jobpb.PrepareJobRequest{
		Pipeline:        p,
		PipelineOptions: nil,
		JobName:         getJobName(opts),
	}
	prepResp, err := client.Prepare(ctx, prepReq)
	if err != nil {
		return nil, err
	}

	//log.Infof(ctx, "Prepared job with id: %v and staging token: %v", prepID, st)

	// (2) Stage artifacts.
	// ctx = grpcx.WriteWorkerID(ctx, prepResp.GetPreparationId())
	// cc, err := grpcx.Dial(ctx, endpoint, 2*time.Minute)
	// if err != nil {
	// 	return "", errors.WithContext(err, "connecting to artifact service")
	// }
	// defer cc.Close()

	//log.Infof(ctx, "Staged binary artifact with token: %v", token)

	// (3) Submit job
	runReq := &jobpb.RunJobRequest{
		PreparationId:  prepResp.GetPreparationId(),
		RetrievalToken: prepResp.GetStagingSessionToken(),
	}

	// slog.InfoContext(ctx, "Submitting job: %v", prepReq.GetJobName())
	runResp, err := client.Run(ctx, runReq)
	if err != nil {
		return nil, fmt.Errorf("failed to submit job: %w", err)
	}

	//slog.InfoContext(ctx, "Submitted job: %v", runResp.GetJobId())

	handle := &Pipeline{
		pipe:   p,
		jobID:  runResp.GetJobId(),
		client: client,
		close: func() {
			cc.Close()
		},
	}

	return handle, handle.Wait(ctx)
}

// WaitForCompletion monitors the given job until completion. It logs any messages
// and state changes received.
func WaitForCompletion(ctx context.Context, client jobpb.JobServiceClient, jobID string) error {
	stream, err := client.GetMessageStream(ctx, &jobpb.JobMessagesRequest{JobId: jobID})
	if err != nil {
		return errors.Wrap(err, "failed to get job stream")
	}

	mostRecentError := "<no error received>"
	var errReceived, jobFailed bool

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				if jobFailed {
					// Connection finished, so time to exit, produce what we have.
					return errors.Errorf("job %v failed:\n%v", jobID, mostRecentError)
				}
				return nil
			}
			return err
		}

		switch {
		case msg.GetStateResponse() != nil:
			resp := msg.GetStateResponse()

			//	slog.InfoContext(ctx, "Job state update", "job_id", jobID, "state", resp.GetState().String())

			switch resp.State {
			case jobpb.JobState_DONE, jobpb.JobState_CANCELLED:
				return nil
			case jobpb.JobState_FAILED:
				jobFailed = true
				if errReceived {
					return errors.Errorf("job %v failed:\n%v", jobID, mostRecentError)
				}
				// Otherwise we should wait for at least one error log from the runner.
			}

		case msg.GetMessageResponse() != nil:
			resp := msg.GetMessageResponse()

			// text := fmt.Sprintf("%v (%v): %v", resp.GetTime(), resp.GetMessageId(), resp.GetMessageText())
			// slog.Log(ctx, messageSeverity(resp.GetImportance()), text)

			if resp.GetImportance() >= jobpb.JobMessage_JOB_MESSAGE_ERROR {
				errReceived = true
				mostRecentError = resp.GetMessageText()

				if jobFailed {
					return fmt.Errorf("job %v failed:\n%w", jobID, errors.New(mostRecentError))
				}
			}

		default:
			return fmt.Errorf("unexpected job update: %v", prototext.Format(msg))
		}
	}
}

func messageSeverity(importance jobpb.JobMessage_MessageImportance) slog.Level {
	switch importance {
	case jobpb.JobMessage_JOB_MESSAGE_ERROR:
		return slog.LevelError
	case jobpb.JobMessage_JOB_MESSAGE_WARNING:
		return slog.LevelWarn
	case jobpb.JobMessage_JOB_MESSAGE_BASIC:
		return slog.LevelInfo
	case jobpb.JobMessage_JOB_MESSAGE_DEBUG, jobpb.JobMessage_JOB_MESSAGE_DETAILED:
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}

type Pipeline struct {
	pipe *pipepb.Pipeline

	jobID  string
	client jobpb.JobServiceClient

	close func()
}

func (p *Pipeline) Wait(ctx context.Context) error {
	return WaitForCompletion(ctx, p.client, p.jobID)
}

func (p *Pipeline) Cancel(ctx context.Context) (jobpb.JobState_Enum, error) {
	request := &jobpb.CancelJobRequest{JobId: p.jobID}
	response, err := p.client.Cancel(ctx, request)
	if err != nil {
		return jobpb.JobState_UNSPECIFIED, errors.Wrapf(err, "failed to cancel job: %v", p.jobID)
	}
	return response.GetState(), nil
}

func (p *Pipeline) Metrics(ctx context.Context) (*Results, error) {
	// TODO cache on job completion.
	request := &jobpb.GetJobMetricsRequest{JobId: p.jobID}
	response, err := p.client.GetJobMetrics(ctx, request)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get metrics for : %v", p.jobID)
	}
	return &Results{pipe: p.pipe, res: response.GetMetrics()}, nil
}

type Results struct {
	pipe *pipepb.Pipeline
	res  *jobpb.MetricResults
}

func (r *Results) UserCounters() map[string]int64 {
	cs := map[string]int64{}
	for _, mon := range r.res.GetCommitted() {
		if mon.Type != "beam:metrics:sum_int64:v1" {
			continue
		}
		un := r.pipe.GetComponents().GetTransforms()[mon.Labels["PTRANSFORM"]].GetUniqueName()

		key := fmt.Sprintf("%s.%s", un, mon.Labels["NAME"])
		v, _ := protowire.ConsumeVarint(mon.GetPayload())
		cs[key] = int64(v)
	}
	return cs
}

func (r *Results) Committed(name string) int64 {
	for _, mon := range r.res.GetCommitted() {
		key := fmt.Sprintf("%s.%s", mon.Labels["PTRANSFORM"], mon.Labels["NAME"])
		if key != name {
			continue
		}
		v, _ := protowire.ConsumeVarint(mon.GetPayload())
		return int64(v)
	}
	return 0
}
