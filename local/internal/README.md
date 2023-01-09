# TODOs - no particular order.

* Beam Model Changes
  * Fix Datalayer provision and lookup to allow for multiple bundles.
    * Needs to be watermark aware to allow window closing & trigger handling.
    * TestStream
  * Remainder of state handling.
  * Composite Handling
    * Real SplittableDoFns (split requests, etc)
      * Different split kinds (channel, residual, separation harness test)
      * Process Continuations & Bundle Rescheduling
  * Stager Refactor
    * One PTransform : One Stage
    * Fusion Stager
* Perf or Infra changes (need to profile first)
  * pullDecoder refactor 
  * []byte avoidance -> To io.Reader/Writer streams
  * Error plumbing rather than log.Fatals or panics.

# Notes to myself: 2023-01-08

After a bit of a struggle, I was able to get a global window process continuation
pipeline working on the runner!  Turns out that if you forget to actually keep the
data weights straight, and have the correct things on the inside of the residual loop
it all fails to terminate.

Right now the "window closing" is implied by there being no more data to process.
This isn't correct outside the global window.

Which means what I need to do next is parse out data elements more than I have been.
Currently it happens around aggregation points, but it needs to happen at each bundle
boundary, in the data layer. It's hard to keep all the separations in my head.

I've got the concept of a stage, which represents a bundle descriptor.
A stage can have one or more bundles, which represent data to be processed.
The data can be one or more elements. The elements can be part of one or more
windows, and have a timestamp.

What I don't have yet is separating data into multiple bundles to be processed simultanously.
Even without the watermark handling, that also allows for the begining of dynamic splittings.

But for any of those, we need to parse out the windowed value header.
The header has the windows, for use in aggregations, the pane, which isn't useful on the 
receiving end in the runner but needs to be maintained between aggregations, and the timestamp
which is part of the element metadata.

Each windowed value needs to go through the watermark estimator, which provides a tentative
estimate until the bundle is complete, which would need to be overridden based on the
progress estimates.

Then those receiving estimators would need to signal the correct downstream aggregations
to process the closed windows. Something Something Triggers.

------

If I feel really saucy, I could try to hack in a Web UI visual display of stages & progress, which
requires at least putting the various levels of progress into a single store.
But that's a big refactor for later.

# Notes to myself: 2023-01-01

I did a little bit of cleanup and unification for the generation thing, and got it
plumbed all the way through.

It occurs to me that for continuous processing, I need to do the packaging for fusion and
staging first. Each generation will be in it's own place in the pipeline.
 that each generation of a process can flow through the runner independantly.
Basically, the returned bundle needs to know which stage it represents, so the next stage can
be kicked off correctly from the previous one. So in addition to the generation, each bundle
needs to have a stageID mapping. For convenience, this will be the index into the topologically
sorted stage slice.

This approach, while correct due to topological sorting, will end up having problems with 
respect to multiple transforms with Process Continuations. The generation numbers will
get mixed up... So that means that each impulse would need to begin it's own subtree,
with runner side flattens, and GBKs doing the same, as they wait for their input to be ready
by watermark advancement.

buildProcessBundle is going to need to take something other than a single transform....

Oh boy. It's beginning to get complicated.

# Notes to myself: 2022-12-31

I cleaned up the urns, and deleted some long unused helper code. Using a generic factory function and a
quick interface, I could get the urn extractor helpers to nice and compact and less error prone to add a new one.
Just call the factory with the enum type, and you're good!

I noted that most of the different features of ParDos are also duplicated
as requirements, so we can handle some of those centrally in handlepardo.go.

The go files are nearing 4k lines of code and comments and blank space which
means we're edging towards when I should migrate this to the repo. It all
needs to be reviewed. 

I think the burndown list for finishing this phase of work is 
ProcessContinuations, Dynamic Splitting, the Configuration set up & use. 

State wouldn't be hard to do. I don't think Timers would be too hard, though
then we want to have "time acceleration" or similar features to help with testing. Triggers shouldn't be too bad, but TBH might as well wait for state
and timers, so It can build on those mechanisms. The configuration bit, should 
enable and disable the various "requirements" features, so that we can emulate different Runners.
Then there's the different data handling things, like State Backed Iterables, which would need to be eminently configurable, in terms of datasize thresholds, page size and the like, and whether to force them to be enabled or not for testing purposes.

ProcessContinuations allow a bundle to checkpoint itself, so not only
can it be restarted with the residuals, but also so that its children can
begin downstream processing simultaneously. Aside from watermark handling to
estimate if a given window should be closed or not at a GBK, I'll need to
propagate through single transform PCollections. This affects the dataService API.

The quick thing that comes to mind is that when a process bundle request returns with residuals and such, the residual bundles should be assigned an
incremented generation number, which is stored with the data. This way up until a GBK, where we need to do watermark based processing, we simply start
the next thing, and request the data for the same generation's parent.

GBKs and Side Inputs in that scenario need to operate a little differently,
since they will aggregate multiple previous generations of data. They don't 
follow the same generation number scheme. A given GBK will be blocked on processing until at least one window is completed. Then it's generation number will be something different. Probably the latest generation's data
that's being used in the aggregation.

That's enough thoughts for now. I'll sleep on it, and figure it out next year.

# Notes to myself: 2022-12-30

Switching GBKs to handle windowing, worked out pretty easily, and so did adding Session windowing.
That was followed up with sorting out the windowing for side inputs. Both were made easier since
I could just pull the self validating integration test pipelines.

The next thing on the list is Process Continuations and watermark handling, but I need to
decouple the datalayer from the bundles, and that's probably going to be a small mess.

Right now bundles retain their output data independantly, which has a problem when there
are multiple bundles contributed data to the PCollection. So we need to fix up the data
layer to abstract that out. Basically we should just key these things on the global Pcollection
ID. This would probably simplify all the mess I have around data coming in and exiting

-----

That data layer needs to be where the watermark awarness handling needs to go, since technically
that's how windows get closed and packaged for later.

Triggers are also very much tied to that layer as a result. IIRC it's possible to treat these
with windowed handling.

How does that look? We get the window headers & timestamps for all the elements.
The timestamps help us estimate the watermark position, unless we have real estimates from the SDK.
We need to map from the data output to the globalID. But we only have the 
instructionID (AKA which bundle it's coming from), and the transformID of
the datasink for that output. The bundle struct needs to have that mapping. 

# Notes to myself: 2022-12-29

Nerd sniped myself into adding MultiMap side input support, so that's in. This reminded me
that I largely punted on any Windowing handling so far. That affects GBKs and Side Inputs.

Also, in implementing things, I learned that the protocol also has a way to extract all
keys from a multimap side input. Hot dang.

There were also some things to move and cleanup that I missed in the initial handler
migration.

I think at this point the main things that the direct runner do that aren't yet supported is
handling windowing to some degree. We certainly have it beat on serialization.  But this
runner doesn't yet handle dynamic splitting, process continuations, and sending multiple bundles,
to an SDK simultanously, which are certainly the trickier parts to resolve.
I want dynamic splitting, and continuations for the testing benefit for the SDK, the latter especially
for users. Multiple bundles does also satisfy a SDK testing concern.

General State API handling is simple and straight forward, since we already force a single bundle with the given keyed state.
Timers remain a mystery, and are handled through the datalayer.

Then of course, there's Cross Language, which largely just means using/building containers, and handling
the environments that way.

So the right ordering is probably:
  * Add windowing.
  * Add process continuations + watermark handling.
  * Add triggers.

----

Reading back through my notes. Windowing was something I intended to do back in February too, and then
it got lost in the Too Busy Shuffle of 2022. Better get on it then.

# Notes to myself: 2022-12-26

I think to clean up execution, the loop needs to be unaware that the
runner is special. It needs to treat it as just another worker with the
environment id "".

This implies that we would have the "environments" be responsible for multiplexing
requests onto actual worker, even though the manager will only have a single FnAPI
server for the various services. This would permit a distinction between the multi
processing SDKs like Go and Java, and the single/sibling processing SDKs like Python.

This set up also allows for correct/individual handling for each environment to cooperate
with the capabilities of the SDKs, rather than the runner deciding this ahead of time.

Some of the complexity is reduced for the preprocessor style handling, since Combines
or SDF composites are understood by SDKs iff the SDK sends the appropriate explicit signals.
And handling features like Stable input, is still functionally runner side.

Multilanguage conflicts with this model though, since Beam allows for a single bundle
executing across multiple environments. But that could also be a separate optimization,
with the layer that does fusing being aware of environments and handling the environmental
fusion correctly.

The runner only transforms will only be installed into the "runner" worker, but flattens
can electively be handled SDK side, so that would be something to be configurable.

------

For handling streaming, it will be OK to start off with the current batch set up for data.
This runner is for testing completeness not optimal processing. We can processess each
bundle one at a time, rather than having all transforms execute simultaneously, or at least
pipelined. It all comes down to data management and pipelining, with how we ensure things
coordinate around windowing and aggregations.

-----

Did a little bit of cleanup on the part that executes a transform on a worker or in "the runner".
Needs a little more before it's cleanly factored, which would simply let the environment
implemenentations handle if it's in the worker, or over the FnAPI.

We currently pro-actively send the bundle descriptor, but that's something we should be toggling
via a configuration as well, for complete testing, with the SDK sending things over.
Closer to runner handling, which is kind of unique, it'll likely be built and configured but in a
way that avoids if statements.

That's the next task. Committing to the configuration and allow variant testing to occur.

# Notes to myself: 2022-12-24

It's the day before Christmas and all through the house
lostluck was was stirring, tapping away and using a mouse.

Anyway...

The Runner can now to a batch SDF with initial splits!
At least, once a small inconsistency is resolved in the SDK.
Extracting the main eleemnt in the main process element split doesn't
work with the current Single Transform approach, because the SDK doesn't
cover for wrapping main element from the fullvalue on decode, when it's
not a KV. This causes a type error.

Basically, this is one of the reasons direct runners are bad. Things like
this sneak in, even though each transform is well defined. The patch for
the Go SDK to resolve this is small however, so that's fine.

But with this in, we add a separation harness test, and force a clean up
of the data layer of this runner. Or more likely, clean up some of the
processing at execution time, moving and refactoring some of the data
handling logic. Only ParDos can have side inputs, so it makes sense to
move that handling into a ParDo specific handler.

I think nearly every other transform (most SDF parts, all combine parts)
only do a single striaght map in, map out transform, which would simplify the
bundle logic. I don't love needing to list every URN with SDK handling like
that explicitly.

Technically what we have there is the no-op fuser. This is fine for now.
We do want that to be used. It's the topological loop. Right now that's
directly a `[]string`, so it's limited to a single transform at a time.
But now might be the time to fix that abstraction, before fixing the
rest of the handling. It would be good to isolate a transform 
concatenating loop, that builds up from the handled transforms from the given
proposed bundle descriptors, and connecting them together with the
Datasource and Datasink transforms. 

The main sticking point there would be changing the bundle/parents logic.
Right now they're very coupled, and it's quite subtle. I don't have a
good idea on that. It needs to be coupled with the fuser approach, since
the fused transforms will have to handle where side inputs go and that
the topological sort remains sound.

# Notes to myself: 2022-12-21

After three weeks of interrupts, not much has happened, but I did get the configuration code done
so handlers can declare what their base characteristics are and have everything properly parsed and validated.

Now I have to use it.

But I can't do that without changing how we generate bundles. IIRC, it currently does a single iteration through
the pipeline and produces and handles each of the bundle descriptor/transforms/stages immeadiately. This isn't
so useful in the long term. Really this needs to be done ahead of time. Further, the handlers themselves need
to "know" how to deal with their various transform URNs. 

So it should flow like this:
  Pipeline passed into the transform Handlers to produce individual transforms.
  Individual transforms are passed to the fuser handler to produce fused stages (currently a no-op)
  Fused stages are sent to worker(s) for processing.

-----

Part 2:
Originally intended to do SDF, but got pulled into a lifted combiner or not instead. 
Turns out composites involve creating synthetic coders, pcollections & transforms, which complicates things.
So to simplify, I'm now cloning the set of components from the original pipeline and mutating those as needed.
The bugs I iterated on were largely about making sure all the places were updated properly, so they'd have
the new components.

Other bits were not using the parameters to the component helper function I wrote, leading to strange SDK side
bugs. There are definitely some improvements to be made to some of the SDK side error messages, around drop keys 
and so on, WRT combiners. The errors don't make it obvious what's wrong. eg. Passing KVs to beam.Combine should
have a clearer error message, directing the user to beam.CombinePerKey instead of it's current mess.

I forgot that the code was already doing everything in advance, mostly, and topologically sorting the outputs.
That makes some things much simpler for any later fusion handlers.

SDFs have the same composite handling that the Combiner Lift needs, but can also trigger splits. 
The first implementation doesn't need to do dynamic splits, but we should be getting the initial splits clearly.
Dynamic splits would require cleaning up the datahandling properly and getting progress updates.

The plus side was that there is plenty of debugging output to figure out what's going on. Huzzah!
But unforutately I am not yet able to move these things to their own package.
Mega package it is for not.

# Notes to myself: 2022-11-30

Finally have time to get back to this.

Aside from the TODO list above, I need a better factoring, in particular in how to get things to be
modular. If it's not done early, we run into the same problems the other runners have, with "one way"
to run and execute things.

I need to add something that will split things into stages ahead of time.
Currently we walk the pipeline and produce bundles straight off of that.
This is OK, but prevents the desired modularity. 
I'd like to have the current mode of "every transform's a stage", and a rudimentary fusing mode.
This is probably trickier than it sounds, since we'd want to be able to do certain bits of common work between the two of them.

The other idea recently be to have the configurations for these in YAML, and be loaded/validated at startup time.
This would allow simpler configuration in the pipeline options.
It would need to be sort of recursive, since there could be specific configuration for each.
It would be delightful to get some kind of powerset thing going with the combination of configuration options.
I just need to keep the mutually exclusive options straight.

Nominially, it's all back to a "handler" set up, with some self awareness. 
Eg a single handler could know "both" ways of expanding and staging things. Like lifted vs unlifted combines.
Then a single configuration file, which could have several named configurations has different "modes" that
could be tested under. Something that replicates Flink's behavior, or Dataflow's, etc.

The name is then all it takes to have the runner execute with those characteristics.

Characteristics isn't a bad name for the details of these variations actually....

# Notes to myself: 2022-06-06

I forgot that composites are the special urns. Otherwise all I'm ever dealing with is
Go DoFns at the leaf level. It's Go DoFns at the bottom, unless it's a runner transform.

That means that I need to do better at the graph pre-processing. 

One of the nice things about the design of the Pipeline Proto is that the leaves are generally
what any runner can execute. However, processing the composites and their urns let one do 
more clever things if necessary, such as combiner lifting, or even "splittable" dofns.

Either way, that's something that can be pulled out. But I do want to "unify" the handling
of Combiner handling, and SDF handling so that these could be configured on a per transform basis
per pipeline, as well as defining a default behavior set.

eg. Don't lift combines on unbounded pcollections, but do lift bounded pcollections transforms.

For now, shard that portion out to it's own file, so it can also eventually be unit tested. This will lay the groundwork to giving this runner the desired configurability.

# Notes to myself: 2022-06-05

Logger has been cleaned up to do a bit of VLogging. Not amazing, but will
let me follow the sequence of events more easily when something goes wrong.

I have a few changes to make to the SDK to ensure a tidy cleanup of goroutines.
Likely won't make any difference in any performance sense, but it'll make me
happier at least. 
Oh and switching loopback execution to no longer be graceful, does the test.

That along with additional De-noising of SDK side execution logs, and splitting
the launcher logs from the Runner logs will make for a better experience.


# Notes to myself: 2022-06-03

OK, it's been a while, and I've promised to write a talk about this. 
I need to work on the code a fair bit.
In particular, I need to clean it up! I have ideas! A vision!
A test runner that can be configured, possibly per pipeline, but
importantly, per URN (if not per transform).

Either way, I'll probably also want to delete the cribbed things from the
direct runner.

But I guess, to start, I need to do something about the flake when executing side inputs...

~some time later~

OK. The problem with the flaky side input execution had to do with how side inputs were
being added in an incorrect order. Given that only Go has an ordering requirement, that
means the layer that's sending data through is incorrect, and can cause a spurious block.

OK. That's sufficient for the night.

Running large batches with -count and -timeout is handy. 
It's revealing there's a cleanup issue with the GRPC stuff that's leaving a number of dead
grpc balancers. Need to see how it's closed.

---- 

The balancer goroutines are from the harness package, since we don't cancel the 
context and close the connections after harness termination. So that's in the main
beam execution.

Otherwise, there was a race condition with creating & using some of the channels.
Easy to fix.

A whole bunch of logging changes, to clean things up. I want to make the debug 
handling a bit more modal, so I don't need them for every test run. This will also
let the logging for later development be easier too. Especially once we start
doing additional configuration work.

# Notes to myself: 2022-02-21

Figure I should keep the list of extant tasks at the top of this file,
instead of losing them in the stream of notes. In looking at the flaky trace
I see too many lefover Data channels waiting on a channel receive. I need
to close the channel!

I am also clearly missing a connection somewhere, as there are a number of 
leftover GRPC connections/transports that need addressing.
Fully closing the listener seems to make the hanging flake that happens 
at the side input test pretty regular like. Since I want to nip this in the
bud, I'll commit this so it comes up more often and force me to do a
proper package refactor.

# Notes to myself: 2022-02-20

Not too bad to add iterable KV handling.

Now there seems to be a bit of contention WRT either the additional
tests, or worse, some non-determinism in the runner or the SDK.

Other than that, Multimap side inputs are next on the docket, and
various cleanups I think. The better logging set up might be the
right way to figure out the reason for the flakes. It's more
likely to be in this code than the SDK, but who knows?

There's lots of refactoring to do too, to better segment the
processing, and contexts for passing things down.

Then we start to need to deal with Windows and all they entail.

# Notes to myself: 2022-02-19

After a bit of weirdness with my mod cache in what seems to be
because of using go1.18beta1, resolved though a 
`go clean --modcache` and moving to RC1, I'm now confident I'm
using the 2.37.0-RC1 of the Beam Go SDK. It was a very strange
error where it was looking like I was using some earlier version
that didn't yet have real iterable side inputs, which is what
I ultimately wanted to produce today.

It was frustrating, as not only was I not seeing any debug
messages I was expecting, I was seeing some changes.
Very strange indeed.

Several Minutes Later

Ultimately it's because I'm an idiot who was using the new Go workspace
feature to refer to the SDK deps and forgot it. Wow. Real dumb.
After commenting out the line, everything started to work as expected with
the expected results.

Had proper Iterable side inputs not been added to the Go SDK in 2.37
it's likely I wouldn't have noticed for quite some time.

Anyway. Similarly to how data is being handled, I need to add striping
of the windows, and re-concatenating the data to an iterable. This should
also work for KVs if I have the type be length prefixed by the SDK side
first.

# Notes to myself: 2022-02-14

Flatten done! 

Now to choose: Side Inputs, or Multiple Outputs?

---

DoFns with Mutliple Outputs apparently. Needed to start the
cleanup of the ad-hoc "_sink" tag I was using. Presently I
hardcoded in what the Go SDK uses by default `i0`, which is
not optimal. We really want to be pulling those from the 
transform ids themselves at least.

This is why we have incrementing layers of tests, since when
something breaks, you just start running the simpler ones first,
and get back to fixing those.

The next bit is side inputs, which requires begining to implement
State service protocols, which means another layer of management.
Likely, doing this the same as the Data service, as they are
largely similar, but care will need to be taken for lock contention.

Another thing todo is to remove the various logger.Fatals, and convert them
to errors. The Fatals ruin the unit test runs, stoping them dead.

The other idea is to make the logging more intentional, so I can have
all the debug logging for testing the runner, but eventually selectively
enable certain logs when running it as stand alone if desired.

# Notes to myself: 2022-02-13

After a hiatus, I returned to get the dang "fork" working at least.
Messy, but it works, and that's the important bit. It involves setting up a map from a user transform to the bundles that generated
their source data.

I have a plan to begin sorting out side inputs, and flattens too.

# Notes to myself: 2022-01-30

Lets return metrics back to the user program, through
the job management API. Factoring things is the trickiest.
Opting now to simply have committed and tentative be different
stores, with each type knowing how to make it's own proto.
But then the protos loook all the same, other than the payload
and urn, so we may change that later to avoid the dep creep.

Not a huge deal for now. Between the overcount bug, and how big
the metrics file is getting, we'll likely need an internal package
to manage things for efficiency's sake.

Once we add tests in, and fix the bug, I'll be done with metrics for
the time being, and work on additional graph shapes.

------------

First up, sinks. That is, DoFns with no Outputs. They end up representing
the last bundle. Just need to be able to handle outputs properly, and start
prepping for local output tagged outputs, and critically having the data
wait set to 0.

Next up, is the harder bit: Downstream dependencies.
We can't throw away a bundle's data until all of it's successors are done.
This will also prep us for having side inputs next too.
Gonna look at the buffering code in the direct runner for inspiration there.
At which point I might be able to delete what I ported over from it.

# Notes to myself: 2022-01-29

Started the day off by preventing other packages
from depending on the server internals. Let's avoid
unnecessary dependants!

Next up, lets finish the metrics thing. We need to
be able to pass the bytes to the accumulator directly
and not manage the types except in abstract.

We'll need to do something different for the progress
and similar metrics if we ever begin taking advantage
of them for splitting or similar, but that's a later
concern.

My goal for today is to get to decoding the payloads,
logging them and shoving everything into a map.
Extraction and can happen next time.

Last notes for the night: Because of the way I'm re-using pcollection
IDs as the data source and sink, and not replacing the
ids for inputs or outputs of the single transform bundles
we end up doubling the PCollection metrics. We either need
to do the re-writing (leading to bonus metrics for the 
logical pcollections) or filtering out the SDK metrics
for Sources. We should also do some basic validation with the metrics
too, like ensuring the final DataChannel count matches the number of
elements we sent.

# Notes to myself: 2022-01-28

I didn't want to have to hardcode all the dang metrics urns
so now it comes down to decomposing specs to their label 
keys. Ideally this will allow using a single dang map
for all the metrics. 

ToDo the same for the payload decoding and storage types
which I've got the barest bit for. But now we have keys,
which is half way there.

At best, this allow us to only need to add new required 
label combos for keys, and decoding/storage types, and then
the system should be able to take care of itself.

But that's a different problem.

# Notes to myself: 2022-01-25

Metrics? What's a metrics to me? What's a metrics to you?

There's the legacy old style metrics, and the new ones. 
The Go SDK implements both, but I've not made it work
with Dataflow, but it's not clear if there's an SDK issue
or a Dataflow issue somehow. I know the SDK isn't sending
all the related protocol urns though.

But for now we can ignore those. I'll first implement the
new ones, and then the old ones, and we should be able to
compare the results.

The question of course is how to store them... do I just
re-use the SDK side store? Will that be problematic?

-----

I think I should also rename "runner.go" to "job.go" since
that's where the job type is living right now. Then I can
probably just rename the whole package from "server" to 
"runner" since that's what this is.

----

At long last, I've figured out the right way to deal with
the proto extensions/options. They are implemented as a
proto extension to EnumValueOptions, which then refer to 
a specific type. This saves me from having to write them out
myself at least. Once more at any rate.

TBH I'm a little annoyed the proto didn't go *farther* so even
more of this could be automated.

# Notes to myself: 2022-01-23

Now that we have a GBK, we could run a simple word count.

I think next the trick is to implement handling composites.

And multiple outputs. Then side inputs.

Or just do metrics?

Composites: Find and thow them out, then re-topological sort.
OK that worked as expected.

gbrt seems to be able to run mostly now, outside of fixing the
GBK to handle more coders. Since most types don't have a length
prefix, I'm getting to use a io.TeeReader to avoid re-encode
steps as we're parsing. I think it might be possible to pass around
readers or byte.Buffers instead. Done correctly, I might be able
to avoid doing too much copying of data around and avoid too much
GC overhead.

Adding a test to make sure that I'm not losing any bytes from the
TeeReaders.

It occured to me that I can simply tell the SDK to have any coder
I haven't implemented yet be length prefixed. #Efficiency!

Done and done. local now execute gbrt! At small scales.
GBRT remains a fancy wordcount.
Next task is probably dealing with metrics, and then multiple outputs,
and once side inputs are handled, this will be about on par with the
direct runner.

# Notes to myself: 2022-01-22

OK. GBK time. This is where things start to get complicated,
as the runner will need to parse the data received, and do
something about it.

Returned values are windowed values.
The element type is a KV.
Overall the window bytes and the key bytes are the important bit
for grouping, and the GBK is ultimately when a Trigger is determined.

Since right now I'm focused on batch, and in memory execution,
but I don't want to install a foot gun on purpose,
this means that I'll be doing multi layers of map.

Top layer is the map for Windows to Aggregations.
Then it's a map from Keys to Values+Timestamps.
Then timestamps will be dropped, but I want to keep them around for now.

But before that, lets make these unit tests actually validate we're getting
the expected data! Using DoFns themselves.

# Notes to myself: 2022-01-21

Moved some things around, but the overall execution hierachy is 

User submits Pipeline to Runner.
Runner turns Pipeline into a Job.
A Job is run on Workers.
Workers can have a number of Environments.
Environments process Bundles.

The main place where it would all fall apart is when a given
Environment has hardware requirments like GPUs and other
accelerators, which almost make me want to reverse the
relationship. It feels like an awkwardness of the model.

Otherwise, the local runner can now execute linear strings of
DoFns! Did some consolidation and cleanup too.

Toss up between branching and GBKs next. Probably GBKs.
Branching requires building up the bundle dependencies
better.

# Notes to myself: 2022-01-18

Before any of the actual pipeline execution, I think sorting out
*proper clean* worker shutdown behavior is likely best. As it stands
things shudwon and tests fail, regardless of what the pipeline is doing.

Also, we can just do an unoptimized runner to start, one DoFn at a time,
and then work on an optimized version. Much simpler.

By the end of the evening:
I have a successfully stopping worker & SDK. Turns out shutting down
properly is the best move. Huzzah.

Next up, handling "Impulse" (which uses a global window non firing pane empty byte.),
and a DoFn, and getting the DoFn output from the sink.
(Everything should be sunk, and not discarded.)

# Notes to myself: 2022-01-17

At this point I have a runner that actuates the SDK harness
and sends & receives data.
However it's presently all a lie, with the source and sink hard coded in.

So there are a few things to do:
1. Persist and organize ProcessBundle instructions, and make managing them
a bit easier to handle.

2. Start Graph Dissection.
This is the harder ongoing work, since I need to take the pipeline and
have the code *plan* how it's breaking things into bundles.
We can't simply take the direct runner code exactly, since it's geared up
as a single bundle runner. This won't be. It'll have multiple bundles.
However, it might only ever have/use one per "stage", and only run one
at a time for the moment.
But that might be too tricky without...

3. Targetted graph subsets.
The ray tracer is certainly too complicated to start with at present.
Lets instead go with smaller pipelines with purpose.
As unit tests of a sort.
Simplest: Impulse -> DoFn -> Sink
Sequence: Impulse -> DoFn -> DoFn -> Sink
Split 1: Impulse -> Sink A
               |--> Sink B
Split 2: Impulse -> DoFn -> Sink A
                       |--> Sink B
Split 2: Impulse -> DoFn -> Sink A
                       |--> Sink B
Grouping: Impulse -> DoFn -> GBK -> DoFn -> Sink
  Probably involves a bit of coder finagling to extract keys and re-provide as inputs and the like.
Combiner Lifting
 This requires understanding the combiner components, and their IOs and handling them properly.
 But vanilla combines are handled by the SDK harness.
SplittableDoFns
 This requires similarly understanding the components and calling them out.
 However also handled by the SDK harness.
None of the above Sinks are actual sinks since those don't have a representation in the model.

4. Then of course theres Metrics and progress, which is required for more complex
pipelines. Need to collect them for all the things.
