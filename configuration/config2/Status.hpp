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

#ifndef _CONFIG2_NODESTATUS_HPP_
#define _CONFIG2_NODESTATUS_HPP_

#include <map>
#include <vector>
#include <string>
#include "platform.h"



struct DECL_EXPORT statusMsg {

    enum msgLevel
    {
        info = 0,     // informational messages mainly
        warning,
        error,
        fatal
    };

    statusMsg(enum msgLevel _msgLevel, const std::string &_nodeId, const std::string &attrName, const std::string &_msg) :
        msgLevel(_msgLevel), nodeId(_nodeId), attribute(attrName), msg(_msg) { }
    msgLevel msgLevel;                // Message level
    std::string nodeId;               // if not '', the node ID to which this status applies
    std::string attribute;            // possible name of attribute in nodeId
    std::string msg;                  // message for user
};


class DECL_EXPORT Status
{
    public:

        Status() : m_highestMsgLevel(statusMsg::info) { }
        ~Status() {}
        void addMsg(enum statusMsg::msgLevel status, const std::string &msg) { addMsg(status, "", "", msg); }
        void addMsg(enum statusMsg::msgLevel status, const std::string &nodeId, const std::string &name, const std::string &msg);
        void addUniqueMsg(enum statusMsg::msgLevel status, const std::string &nodeId, const std::string &name, const std::string &msg);
        enum statusMsg::msgLevel getHighestMsgLevel() const { return m_highestMsgLevel; }
        bool isOk() const { return m_highestMsgLevel <= statusMsg::warning; }
        bool isError() const { return m_highestMsgLevel >= statusMsg::error; }
        std::string getStatusTypeString(enum statusMsg::msgLevel status) const;
        enum statusMsg::msgLevel getMsgLevelFromString(const std::string &status) const;
        std::vector<statusMsg> getMessages() const;
        void add(const std::vector<statusMsg> msgs);


    private:

        enum statusMsg::msgLevel m_highestMsgLevel;
        std::multimap<enum statusMsg::msgLevel, statusMsg> m_messages;
};

#endif
