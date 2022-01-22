
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
