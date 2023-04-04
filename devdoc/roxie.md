# Everything you ever wanted to know about Roxie

## Why did I create it?

Because I could. Many of the pieces needed for Roxie were already created for use in other
systems – ECL language, code generator, index creation in Thor, etc. Indexes could be used
by Moxie, but that relied on monolithic single-part indexes, was single-threaded (forked a
process per query) and had limited ability to do any queries beyond simple index lookups.
ECL had already proved itself as a way to express more complex queries concisely, and the
concept of doing the processing next to the data had been proved in hOle and Thor, so Roxie
– using the same concept for online queries using indexes – was a natural extension of that,
reusing the existing index creation and code generation, but adding a new run-time engine
geared towards pre-deployed queries and sending index lookup requests to the node holding the
index data.

## How do activities link together?

The code generator creates a graph (DAG) representing the query, with one node per activity
and links representing the inputs and dependencies. There is also a helper class for each
activity.

Roxie loads this graph for all published queries, creating a factory for each activity and
recording how they are linked. When a query is executed, the factories create the activity
instances and link them together. All activities without output activities (known as ‘sinks’)
are then executed (often on parallel threads), and will typically result in a value being
written to a workunit, to the socket that the query was received in, or to a global “context”
area where subsequent parts of the query might read it.

Data is pulled through the activity graph, by any activity that wants a row from its input
requesting it. Evaluation is therefore lazy, with data only calculated as needed. However, to
reduce latency in some cases activities will prepare results ahead of when they are requested
– for example an index read activity will send the request to the agent(s) as soon as it is
started rather than waiting for the data to be requested by its downstream activity. This may
result in wasted work, and in some cases may result in data coming back from an agent after
the requesting query has completed after discovering it didn’t need it after all – this
results in the dreaded “NO msg collator found – using default” tracing (not an error but may
be indicative of a query that could use some tuning).

Before requesting rows from an input, it should be started, and when no more rows are
required it should be stopped. It should be reset before destruction or reuse (for example
for the next row in a child query).

Balancing the desire to reduce latency with the desire to avoid wasted work can be tricky.
Conditional activities (IF etc) will not start their unused inputs, so that queries can be
written that do different index reads depending on the input. There is also the concept of a
“delayed start” activity – I would need to look at the code to remind myself of how those are
used.

## Where are the Dragons?

Splitter activities are a bit painful – they may result in arbitrary buffering of the data
consumed by one output until another output is ready to request a row. It’s particularly
complex when some of the outputs don’t start at all – the splitter needs to keep track of how
many of the inputs have been started and stopped (an input that is not going to be used must
be stopped, so that splitters know not to keep data for them). Tracking these
start/stop/reset calls accurately is very important otherwise you can end up with weird bugs
including potential crashes when activities are destroyed. Therefore we report errors if the
counts don’t tally properly at the end of a query – but working out where a call was missed
is often not trivial. Usually it’s because of an exception thrown from an unexpected place,
e.g. midway through starting.

Note that row requests from the activities above a splitter may execute on whichever thread
downstream from the splitter happens to need that particular row first.

The splitter code for tracking whether any of the downstream activities still need a row is a
bit hairy/inefficient, IIRC. There may be scope to optimize (but I would recommend adding
some good unit test cases first!)

## How does “I beat you to it” work?

When there are multiple agents fulfilling data on a channel, work is shared among them via a
hash of the packet header, which is used to determine which agent should work on that packet.
However, if it doesn’t start working on it within a short period (either because the node is
down, or because it is too busy on other in-flight requests), then another node may take over. The IBYTI messages are used to indicate that a node has started to work on a packet and
therefore there is no need for a secondary to take over.

The priority of agents as determined by the packet hash is also used to determine how to
proceed if an IBYTI is received after starting to work on a request. If the IBYTI is from a lower priority buddy (sub-channel) then it is ignored, if it’s from a higher priority one
then the processing will be abandoned.

When multicast is enabled, the IBYTI is sent on the same multicast channel as the original
packet (and care is needed to ignore ones sent by yourself). Otherwise it is sent to all
buddy IPs.

Nodes keep track of how often they have had to step in for a supposedly higher priority node,
and reduce their wait time before stepping in each time this happens, so if a node has
crashed then the buddy nodes will end up taking over without every packet being delayed.

(QUESTION – does this result in the load on the first node after the failed node getting double the load?)

Newer code for cloud systems (where the topology may change dynamically) send the information
about the buddy nodes in the packet header rather than assuming all nodes already have a
consistent version of that information. This ensures that all agents are using the same
assumptions about buddy nodes and their ordering.

## All about index compression

An index is basically a big sorted table of the keyed fields, divided into pages, with an
index of the last row from each page used to be able to locate pages quickly. The bottom
level pages (‘leaves’) may also contain payload fields that do not form part of the lookup
but can be returned with it.

Typical usage within LN Risk tends to lean towards one of two cases:
-	Many keyed fields with a single “ID” field in the payload
-	A single “ID” field in the key with many “PII” fields in the payload.

There may be some other cases of note too though – e.g. an error code lookup file which heavily,
used, or Boolean search logic keys using smart-stepping to implement boolean search conditions.

It is necessary to store the index pages on disk compressed – they are very compressible –
but decompression can be expensive. For this reason traditionally we have maintained a cache
of decompressed pages in addition to the cache of compressed pages that can be found in the
Linux page cache. However, it would be much preferred if we could avoid decompressing as much
as possible, ideally to the point where no significant cache of the decompressed pages was
needed.

Presently we need to decompress to search, so we’ve been looking at options to compress the
pages in such a way that searching can be done using the compressed form. The current design
being played with here uses a form of DFA to perform searching/matching on the keyed fields –
the DFA data is a compact representation of the data in the keyed fields but is also
efficient to use as for searching. For the payload part, we are looking at several options
(potentially using more than one of them depending on the exact data) including:

-	Do not compress (may be appropriate for ID case, for example)
-	Compress individual rows, using a shared dictionary (perhaps trained on first n rows of the index)
-	Compress blocks of rows (in particular, rows that have the same key value)

A fast (to decompress) compression algorithm that handles small blocks of data efficiently is needed. Zstd may be one possible candidate.

Preliminary work to enable the above changes involved some code restructuring to make it
possible to plug in different compression formats more easily, and to vary the compression
format per page.

## What is the topology server for?

It’s used in the cloud to ensure that all nodes can know the IP addresses of all agents
currently processing requests for a given channel. These addresses can change over time due
to pod restarts or scaling events. Nodes report to the topology server periodically, and it
responds to them with the current topology state. There may be multiple topology servers
running (for redundancy purposes). If so all reports should go to all, and it should not
matter which one’s answer is used. (QUESTION – how is the send to all done?)

## Lazy File IO

All IFileIO objects used to read files from Roxie are instantiated as IRoxieLazyFileIO
objects, which means:

-	The underlying file handles can be closed in the background, in order to handle the case
    where file handles are a limited resource. The maximum (and minimum) number of open files
    can be configured separately for local versus remote files (sometimes remote connections
    are a scarcer resource than local, if there are limits at the remote end).
    
-	The actual file connected to can be switched out in the background, to handle the case
    where a file read from a remote location becomes unavailable, and to switch to reading
    from a local location after a background file copy operation completes.

## New IBYTI mode

Original IBYTI implementation allocated a thread (from the pool) to each incoming query packet, but some
will block for a period to allow an IBYTI to arrive to avoid unnecessary work. It was done this way for 
historical reasons - mainly that the addition of the delay was after the initial IBYTI implementation, so 
that in the very earliest versions there was no priority given to any particular subchannel and all would
start processing at the same time if they had capacity to do so.

This implementation does not seem particularly smart - in particular it's typing up worker threads even
though they are not actually working, and may result in the throughput of the Roxie agent being reduced. For
that reason an alternative implementation (controlled by the NEW_IBYTI flag) was created during the cloud
transition which tracks what incoming packets are waiting for IBYTI expiry via a separate queue, and they are
only allocated to a worker thread once the IBYTI delay times out.

So far the NEW_IBYTI flag has only been set on containerized systems (simply to avoid rocking the boat on the
bare-metal systems), but we may turn on in bare metal too going forward (and if so, the old version of the code
can be removed sooner or later).


## Testing Roxie code

Sometimes when developing/debugging Roxie features, it's simplest to run a standalone executable. Using server mode
may be useful if wanting to debug server/agent traffic messaging.

For example, to test IBYTI behaviour on a single node, use

    ./a.out --server --port=9999 --traceLevel=1 --logFullQueries=1 --expert.addDummyNode --roxieMulticastEnabled=0 --traceRoxiePackets=1

Having first compiled a suitable bit of ECL into a.out. I have found a snippet like this quite handy:

    rtl := SERVICE
     unsigned4 sleep(unsigned4 _delay) : eclrtl,action,library='eclrtl',entrypoint='rtlSleep';
    END;
    
    d := dataset([{rtl.sleep(5000)}], {unsigned a});
    allnodes(d)+d;