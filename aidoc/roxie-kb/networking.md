# Roxie Communications & Networking

This document outlines the detailed mechanics of network communications within the Roxie cluster (particularly focusing on `roxie/udplib/`), the dichotomy between Server-to-Worker and Worker-to-Server messaging, and the distinctions between UDP and TCP transport mediums.

## Architectural Overview

Roxie employs an abstracted transport layer consisting of symmetric Send Managers (`ISendManager`) and Receive Managers (`IReceiveManager`). While the names might imply a directional hierarchy, both Roxie Servers and Roxie Workers instantiate these managers to facilitate bidirectional queries and results.

The key boundary files:
- **UDP Implementation**: `udptrs.cpp` (Send), `udptrr.cpp` (Receive)
- **TCP Implementation**: `tcptrs.cpp` (Send), `tcptrr.cpp` (Receive)
- **Collating & Packing**: `udpmsgpk.cpp` (Collator/Packer)

### Transport Boundaries and Flow Control
Roxie's original communication model revolves heavily around custom UDP-based transmission. Because native UDP provides minimal routing capabilities with no guarantees around delivery order, loss, or duplicate transmission, Roxie implements a robust proprietary flow control and windowing mechanism.

* **"Request-to-Send" (RTS) / "OK-to-Send" (CTS) Handshake**: To prevent overwhelming receive buffers on a Roxie Server or Worker, the transmit layer tracks packet queue counts. When a queue transitions from empty to having data (`packetsQueued++ == 0`), a flow-control packet is sent requesting permission to transmit.
* **Flow Management Layout**:
  - `send_resend_flow`: Background thread monitoring timed-out "request to send" tokens.
  - `send_receive_flow`: Background thread listening for "ok-to-send" tokens from the receiver and pushing permissions into a token queue.
  - `send_data`: Background thread dequeuing permission tokens, broadcasting "busy," transmitting the data, and updating the state upon completion.

### Worker-to-Server vs. Server-to-Worker Communication

Both pathways use similar conceptual pipelines (`ISendManager` / `IReceiveManager` abstractions), but practically, their scaling behaviors and typical network topologies diverge:

1. **Server-to-Worker**: 
   When a Roxie Server initiates a query, it multicasts or unicasts to specific leaf node Workers. The payload is typical of query context, filter conditions, and sub-query details. Multicasting sub-queries is naturally fitted to UDP. Since Workers receive sporadic, distinct workloads, their receiving throughput doesn't usually bottleneck unless the query choreography fan-out is huge.

2. **Worker-to-Server (Agent back to Server)**: 
   This path handles result consolidation. Multiple workers concurrently respond back to the central Server that choreographed them. Under heavy loads, this pathway risks "Incast" issues where a Server is flooded by high-throughput UDP packet bursts. The custom RTS/CTS flow control mechanism (described above) avoids UDP dropping by essentially simulating windowing limits on Worker-to-Server pipelines.

## Packet Reconstruction & Message Collating

### The Sequencing Challenge

Since an individual data payload might exceed `roxiemem::DATA_ALIGNMENT_SIZE` or optimal MTU fragments, data is broken down via `IMessagePacker`. On the receiving end, `CMessageCollator` (in `udpmsgpk.cpp`) pieces this together.

* **UDP Reconstruction Mechanics**: 
  When packets arrive at the receive socket, they are placed in an input queue to act as a buffer. A dedicated *Packet Collator Thread* consumes this queue, reading `UdpPacketHeader` boundaries. 
  Because UDP does not guarantee ordering, the collator delegates payloads into a `PacketSequencer`. The sequencer maintains slots mapping sequence IDs to memory buffers (`pkSqncr->insert(dataBuff)`). It actively monitors for:
  - **Holes/Missing Packets**: If subsequent sequences arrive indicating dropped parts, NAKs (Negative Acknowledgments) request resends.
  - **Duplicate Packets**: Sequence collisions increment atomic tracking counters (`totalDuplicates`).
  - **Reassembly**: Once the `UDP_PACKET_COMPLETE` flag is caught and all contiguous slots are full, it considers the message fully reconstructed and yields the databuffer up to the application handler (`IRowManager`).

### TCP vs UDP Collating Differences

Because TCP operates as a reliable stream-oriented protocol, it inherently guarantees in-order delivery, automatic resends of lost packets, duplicate suppression, and sliding-window flow control based on receiver buffer availability. This allows the TCP implementation to bypass much of the complexity required for UDP.

When inspecting the Roxie TCP abstractions (`tcptrr.cpp` and `tcptrs.cpp`), we can observe exactly how the codebase discards UDP's custom complexity:

1. **Direct Collating (`collateDirectly = true`)**: In `CTcpReceiveManager`, the background packet sequencing threaded queues are unnecessary. Packets are parsed inline right off the socket (`collatePacket(buffer)`) and forwarded. There is no `PacketSequencer` checking for NAKs or sequence gaps.
2. **Simplified Sending Context**: In `CTcpSendManager`, the worker threads dedicated to RTS/CTS (`send_resend_flow`, etc.) don't exist. Calling methods like `dataQueued(ruid, msgId, dest)` trivially return `false`, because manual packet scheduling and throttling is completely abandoned in favor of offloading backpressure to native OS TCP sockets (`CSocketTarget::writeAsync`).
3. **No Resend Caching**: The complex tracking structs (`UdpResentList`) that keep copies of recently sent packages to satisfy potential UDP resend requests are completely removed, avoiding memory overhead.

Using TCP not only drastically simplifies the logic (freeing threads and CPU cycles previously spent on packet choreography) but intrinsically addresses incast flooding via native congestion control (CUBIC/BBR). However, it does come with the native OS overhead for managing millions of TCP sockets and head-of-line blocking, which is why Roxie continues to support both.