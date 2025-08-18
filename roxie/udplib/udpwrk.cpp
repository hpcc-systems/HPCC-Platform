/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "udptrs.hpp"
#include "udpipmap.hpp"

#include "jsocket.hpp"
#include "jlog.hpp"
#include "jencrypt.hpp"
#include "jsecrets.hpp"
#include "roxie.hpp"
#ifdef _WIN32
#include <winsock.h>
#else
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
#include <math.h>
#include <atomic>
#include <algorithm>

#include "udpsha.hpp"

class CUdpWorkerCommunicator
{
public:
    CUdpWorkerCommunicator();

    size32_t sendToWorker(size32_t len, const void * data, const SocketEndpoint &ep)
    {
        return workerRequestSocket->udp_write_to(ep, data, len);
    }

private:
    Owned<ISocket> workerRequestSocket;
    SocketEndpointArray multicastEndpoints;
};

