/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef UDPTOPO_INCL
#define UDPTOPO_INCL
#include "jlib.hpp"
#include "jsocket.hpp"
#include "udplib.hpp"

/*
 * IBYTI handling
 *
 * IBYTI (I beat you to it) messages are sent by the agent that is going to process a particular request,
 * to tell the other agents on the same channel not to bother.
 *
 * In order to reduce wasted work, for each request a "primary" subchannel is selected (based on a hash of the
 * packet's RUID) - this channel will process the request immediately, but others will delay a little while
 * in order to give the expected IBYTI time to arrive.
 *
 * The decision on how long to delay is a little complex - too long, and you end up losing the ability for a
 * backup agent to step in when primary is under load (or dead); too short and you end up duplicating work.
 * It's also important for the delay to be adaptive so that if a agent goes offline, the other agents on the
 * subchannel don't keep waiting for it to take its turn.
 *
 * The adaptiveness is handled by noting any time that we delay waiting for an IBYTI that does not arrive - this
 * may mean that the agent(s) we expected to get there first are offline, and thus next time we don't wait quite
 * so long for them. Conversely, any time an IBYTI does arrive from another agent on your channel, you know that
 * it is online and so can reset the delay to its original value.
 *
 * A previous version of this code assumed a single missed IBYTI was enough to assume that a agent was dead and drop the
 * delay for that agent to zero - this turned out to behave pretty poorly when under load, with much duplicated work.
 * Thus we take care to adjust the delay more gradually, while still ending up with a zero delay if the buddy does not respond
 * several times in a row.
 */

/*
 * A "subchannel" is a value from 1 to 7 (with current settings) that indicates which "copy" of the data for this channel
 * is being processed by this agent. A value of 0 would indicate that this agent does not have any data for this channel.
 * In a typical 100-way roxie with cyclic redundancy, node 1 would be processing channel 1, subchannel 1, and channel 2,
 * subchannel 2, node 2 would be processing channel 2, subchannel 1 and channel 3, subchannel 2, and so on u to node 100,
 * which would process channel 100, subchannel 1 and channel 1, subchannel 2.
 *
 * To determine which subchannel is the "primary" for a given query packet, a hash value of fields from the packet header
 * is used, modulo the number of subchannels on this channel. The agent on this subchannel will respond immediately.
 * Agents on other subchannels delay according to the subchannel number - so on a 4-way redundant system, if the primary
 * subchannel is decided to be 2, the agent on subchannel 3 will delay by 1 ibytiDelay value, the agent on subchannel 4 by
 * 2 values, and the agent on subchannel 1 by 3 values (this assumes all agents are responding normally).
 *
 * In fact, the calculation is a little more complex, in that the "units" are adjusted per subchannel to take into account
 * the responsiveness or otherwise of a subchannel. Initially, the delay value for each subchannel is the same, but any time
 * a agent waits for an IBYTI that does not arrive on time, the delay value for any agent that is "more primary" than me for
 * this packet is reduced. Any time an IBYTI _does_ arrive on time, the delay is reset to its initial value.
 */

extern UDPLIB_API unsigned minIbytiDelay;
extern UDPLIB_API unsigned initIbytiDelay;
extern UDPLIB_API SocketEndpoint myAgentEP;

class UDPLIB_API ChannelInfo
{
public:
    ChannelInfo(unsigned _subChannel, unsigned _numSubChannels, unsigned _replicationLevel);
    ChannelInfo(ChannelInfo && ) = default;

    unsigned getIbytiDelay(unsigned primarySubChannel) const;
    void noteChannelsSick(unsigned primarySubChannel) const;
    void noteChannelHealthy(unsigned subChannel) const;
    inline unsigned subChannel() const { return mySubChannel; }
    inline unsigned replicationLevel() const { return myReplicationLevel; }

    /*
     * Determine whether to abort on receipt of an IBYTI for a packet which I have already started processing
     * As I will also have sent out an IBYTI, I should only abort if the sender of the IBYTI has higher priority
     * for this packet than I do.
     */
    bool otherAgentHasPriority(unsigned priorityHash, unsigned otherAgentSubChannel) const;

private:
    unsigned mySubChannel = 0;     // Which subChannel does this node implement for this channel - zero-based
    unsigned myReplicationLevel = 0; // Which data location is this channel pulling its data from - zero-based
    unsigned numSubChannels = 0;   // How many subchannels are there for this channel, across all agents. Equivalently, the number of agents that implement this channel
    mutable std::vector<unsigned> currentDelay;  // NOTE - technically should be atomic, but in the event of a race we don't really care who wins
};

interface ITopologyServer : public IInterface
{
    virtual const SocketEndpointArray &queryAgents(unsigned channel) const = 0;
    virtual const SocketEndpointArray &queryServers(unsigned port) const = 0;
    virtual const ChannelInfo &queryChannelInfo(unsigned channel) const = 0;
    virtual const std::vector<unsigned> &queryChannels() const = 0;
    virtual bool implementsChannel(unsigned channel) const = 0;
};

extern UDPLIB_API unsigned getNumAgents(unsigned channel);
extern UDPLIB_API const ITopologyServer *getTopology();

struct RoxieEndpointInfo
{
    enum Role { RoxieServer, RoxieAgent } role;
    unsigned channel;
    SocketEndpoint ep;
    unsigned replicationLevel;
};

extern UDPLIB_API void initializeTopology(const StringArray &topoServers, const std::vector<RoxieEndpointInfo> &myRoles);
extern UDPLIB_API void publishTopology(unsigned traceLevel);
#ifndef _CONTAINERIZED
extern UDPLIB_API void createStaticTopology(const std::vector<RoxieEndpointInfo> &allRoles, unsigned traceLevel);
#endif
#endif
