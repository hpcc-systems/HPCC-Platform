/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#ifndef DAQUEUE_HPP
#define DAQUEUE_HPP

#ifdef DALI_EXPORTS
    #define da_decl DECL_EXPORT
#else
    #define da_decl DECL_IMPORT
#endif

#include "jstring.hpp"

#define ROXIE_QUEUE_EXT ".roxie"
#define THOR_QUEUE_EXT ".thor"
#define ECLCCSERVER_QUEUE_EXT ".eclserver"
#define ECLSERVER_QUEUE_EXT ECLCCSERVER_QUEUE_EXT
#define ECLSCHEDULER_QUEUE_EXT ".eclscheduler"
#define ECLAGENT_QUEUE_EXT ".agent"

#ifndef _CONTAINERIZED
inline StringBuffer &getClusterRoxieQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ROXIE_QUEUE_EXT);
}
#endif

inline StringBuffer &getClusterEclCCServerQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLCCSERVER_QUEUE_EXT);
}

inline StringBuffer &getClusterEclServerQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLSERVER_QUEUE_EXT);
}

inline StringBuffer &getClusterEclAgentQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(ECLAGENT_QUEUE_EXT);
}

inline StringBuffer &getClusterThorQueueName(StringBuffer &ret, const char *cluster)
{
    return ret.append(cluster).append(THOR_QUEUE_EXT);
}

#endif
