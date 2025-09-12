/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef TCPTRS_HPP__
#define TCPTRS_HPP__

#include "jsocket.hpp"
#include "udplib.hpp"
#include "udpsha.hpp"

ISendManager *createTcpSendManager(int server_flow_port, int data_port, int client_flow_port, int queue_size_pr_server, int queues_pr_server, const IpAddress &_myIP, TokenBucket *rateLimiter, bool encryptionInTransit);

#endif