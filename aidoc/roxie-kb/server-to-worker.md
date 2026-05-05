# Server-to-Worker Communication & Activity Choreography

This document covers the specific pathways and potential bottlenecks for how the Roxie Server dispatches work—such as index reads and keyed joins—to the worker nodes layer.

## Server-Side Remote Execution
When an ECL query executes on Roxie, the graph is chopped into multiple activities. Activities that require accessing distributed index partitions (like Index Read, Fetch, or Keyed Join) are mapped into subclassed "Remote" activities on the server side:

* **CRoxieServerRemoteActivity**: General wrapper that takes a segment of the query graph, packs it into an IRoxieQueryPacket context, and ships it.
* **CRoxieServerKeyedJoinActivity / CRoxieServerKeyedJoinBase**: Controls distributed keyed joins. It holds a head component referencing the remote index read and manages the dispatching of keyed join conditions.

### Network Pathway (ROQ)
When CRoxieServerRemoteActivity (or any activity) dictates sending a workload to a worker, it passes an IRoxieQueryPacket over to the ROQ (Roxie Objective Queue).
- **RoxieThrottledPacketSender::run()**: Dequeues and passes tasks to channelWrite(). It's governed by a TokenBucket for primitive transmission throttling so the servers don’t burst out query instructions faster than green switches or OS UDP receive-buffers can handle. 
- The target worker IP maps are referenced across the UDP topology (udpipmap.cpp / udptopo.cpp).

## Bottleneck Points and Contention (Server-to-Worker)

1. **Throttled Sender Contention (ROQ)**: 
   The server relies on waiting.enqueue(x) and TokenBucket in the RoxieSocketQueueManager to gate communication. If a single bursty query submits too many sub-workloads, the mutex CriticalBlock qc(qcrit) and condition variable wakes (available.signal()) rapidly contend, slowing down Server throughput.
   
2. **Packet Serialization**:
   Each packet->serialize() and subsequent packing occurs inside the single background threaded RoxieThrottledPacketSender, causing a potential single-thread chokepoint before hitting OS sockets (channelWrite). Multithreading the final serialization / socket write steps could pipeline more operations simultaneously.

3. **Packet Transmission Duplication**:
   If multicasting is disabled or TCP is strictly utilized, a high fanout remote activity forces the Server to perform repetitive allocations and dispatches per worker instead of broadcasting.
