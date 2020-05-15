/*##############################################################################
#    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
##############################################################################
 */

#ifndef _ARCHIVE_HPP__
#define _ARCHIVE_HPP__

#include "jstring.hpp"
#include "jptree.hpp"
#include "jtime.hpp"
#include "jfile.hpp"
#include "jutil.hpp"
#include "ws_decoupledlogging.hpp"

class CArchiveTankFileHelper : public CSimpleInterface
{
    Owned<IClientWSDecoupledLog> client;
    StringAttr logServer, groupName;
    StringBuffer archiveToDir;

    IClientGetAckedLogFilesResponse* getAckedLogFiles();
    bool validateAckedLogFiles(IArrayOf<IConstLogAgentGroupTankFiles>& ackedLogFilesInGroups);
    void archiveAckedLogFilesForLogAgentGroup(const char* groupName, const char* tankFileDir, StringArray& ackedLogFiles);
    void cleanAckedLogFilesForLogAgentGroup(const char* groupName, StringArray& ackedLogFiles);

public:
    CArchiveTankFileHelper(IProperties* input);

    void archive();
};

IClientWSDecoupledLog* createLogServerClient(IProperties* input);
void archiveTankFiles(IProperties* input);

#endif
