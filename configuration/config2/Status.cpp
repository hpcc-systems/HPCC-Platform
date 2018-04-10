/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#include "Status.hpp"


void Status::addMsg(enum statusMsg::msgLevel level, const std::string &nodeId, const std::string &name, const std::string &msg)
{
    statusMsg statusMsg(level, nodeId, name, msg);
    m_messages.insert({level, statusMsg });
    if (level > m_highestMsgLevel)
        m_highestMsgLevel = level;
}


void Status::addUniqueMsg(enum statusMsg::msgLevel level, const std::string &nodeId, const std::string &name, const std::string &msg)
{
    bool duplicateFound = false;
    auto msgRange = m_messages.equal_range(level);
    for (auto msgIt = msgRange.first; msgIt != msgRange.second && !duplicateFound; ++msgIt)
    {
        duplicateFound = (msgIt->second.nodeId == nodeId) && (msgIt->second.attribute == name) && (msgIt->second.msg == msg);
    }

    if (!duplicateFound)
        addMsg(level, nodeId, name, msg);
}

std::vector<statusMsg> Status::getMessages() const
{
    std::vector<statusMsg> msgs;
    for (auto it = m_messages.begin(); it != m_messages.end(); ++it)
    {
        msgs.push_back(it->second);
    }
    return msgs;
}


std::string Status::getStatusTypeString(enum statusMsg::msgLevel status) const
{
    std::string result = "Not found";
    switch (status)
    {
        case statusMsg::info:    result = "Info";     break;
        case statusMsg::warning: result = "Warning";  break;
        case statusMsg::error:   result = "Error";    break;
        case statusMsg::fatal:   result = "Fatal";    break;
    }
    return result;
}

void Status::add(const std::vector<statusMsg> msgs)
{
    for (auto msgIt = msgs.begin(); msgIt != msgs.end(); ++msgIt)
    {
        m_messages.insert({ (*msgIt).msgLevel, *msgIt });
    }
}