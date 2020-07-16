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

#pragma warning(disable:4786)

#include "archive.hpp"

CArchiveTankFileHelper::CArchiveTankFileHelper(IProperties* input)
{
    logServer.set(input->queryProp("server"));
    if (logServer.isEmpty())
        throw makeStringException(-1, "Please specify url.");
    groupName.set(input->queryProp("group"));
    archiveToDir.set(input->queryProp("dir"));
    if (archiveToDir.isEmpty())
        throw makeStringException(-1, "Please specify archive dir.");
    client.setown(createLogServerClient(input));
}

void CArchiveTankFileHelper::archive()
{
    printf("Starting archive() ...\n");
    Owned<IClientGetAckedLogFilesResponse> resp = getAckedLogFiles();
    IArrayOf<IConstLogAgentGroupTankFiles>& ackedLogFilesInGroups = resp->getAckedLogFilesInGroups();

    //Check any invalid response and print out the files to be archived just in case
    if (!validateAckedLogFiles(ackedLogFilesInGroups))
    {
        printf("Found invalid response. No file will be archived.\n");
        return;
    }

    ForEachItemIn(i, ackedLogFilesInGroups)
    {
        IConstLogAgentGroupTankFiles& ackedLogFilesInGroup = ackedLogFilesInGroups.item(i);
        StringArray& ackedLogFiles = ackedLogFilesInGroup.getTankFileNames();
        if (!ackedLogFiles.length())
            continue;

        const char* groupName = ackedLogFilesInGroup.getGroupName();
        const char* tankFileDir = ackedLogFilesInGroup.getTankFileDir();
        printf("%s: archiving files from %s to %s.\n", groupName, tankFileDir, archiveToDir.str());
        archiveAckedLogFilesForLogAgentGroup(groupName, tankFileDir, ackedLogFiles);
        printf("%s: files archived.\n", groupName);
        cleanAckedLogFilesForLogAgentGroup(groupName, ackedLogFiles);
        printf("%s: files cleaned.\n", groupName);
    }
    printf("The archive() done\n");
}

IClientGetAckedLogFilesResponse* CArchiveTankFileHelper::getAckedLogFiles()
{
    Owned<IClientGetAckedLogFilesRequest> req = client->createGetAckedLogFilesRequest();
    if (!groupName.isEmpty())
    {
        IArrayOf<IEspLogAgentGroup> groups;
        Owned<IEspLogAgentGroup> group = createLogAgentGroup();
        group->setGroupName(groupName.get());
        groups.append(*group.getClear());
        req->setGroups(groups);
    }
    Owned<IClientGetAckedLogFilesResponse> resp = client->GetAckedLogFiles(req);
    if (!resp)
        throw makeStringException(-1, "Failed in GetAckedLogFiles.");

    const IMultiException* excep = &resp->getExceptions();
    if (excep != nullptr && excep->ordinality() > 0)
    {
        StringBuffer msg;
        printf("%s\n", excep->errorMessage(msg).str());
        throw makeStringException(-1, "Cannot archiveTankFiles.");
    }
    return resp.getClear();
}

bool CArchiveTankFileHelper::validateAckedLogFiles(IArrayOf<IConstLogAgentGroupTankFiles>& ackedLogFilesInGroups)
{
    bool invalidResponse = false;
    ForEachItemIn(i, ackedLogFilesInGroups)
    {
        IConstLogAgentGroupTankFiles& ackedLogFilesInGroup = ackedLogFilesInGroups.item(i);
        const char* groupName = ackedLogFilesInGroup.getGroupName();
        if (isEmptyString(groupName))
        {
            printf("Empty group name in GetAckedLogFilesResponse.\n");
            invalidResponse = true;
            continue;
        }
        const char* tankFileDir = ackedLogFilesInGroup.getTankFileDir();
        if (isEmptyString(tankFileDir))
        {
            printf("Empty TankFile directory for group %s.\n", groupName);
            invalidResponse = true;
            continue;
        }

        printf("Group %s, TankFile directory %s:\n", groupName, tankFileDir);
        StringArray& ackedLogFiles = ackedLogFilesInGroup.getTankFileNames();
        if (!ackedLogFiles.length())
        {
            printf("No TankFile to be archived for %s.\n", groupName);
        }
        else
        {
            ForEachItemIn(i, ackedLogFiles)
                printf("TankFile to be archived: %s.\n", ackedLogFiles.item(i));
        }
    }
    return !invalidResponse;
}

void CArchiveTankFileHelper::archiveAckedLogFilesForLogAgentGroup(const char* groupName, const char* archiveFromDir, StringArray& ackedLogFiles)
{
    Owned<IFile> archiveToFolder = createIFile(archiveToDir);
    if (!archiveToFolder->exists())
        archiveToFolder->createDirectory();

    StringBuffer timeStr;
    ForEachItemIn(i, ackedLogFiles)
    {
        const char* ackedFileName = ackedLogFiles.item(i);
        printf("Archiving %s.\n", ackedFileName);

        StringBuffer ackedFile(archiveFromDir), archivedFile(archiveToDir);
        addPathSepChar(ackedFile);
        addPathSepChar(archivedFile);
        ackedFile.append(ackedFileName);
        archivedFile.append(ackedFileName);

        Owned<IFile> srcfile = createIFile(ackedFile);
        Owned<IFile> dstFile = createIFile(archivedFile);
        if (dstFile->exists())
        {
            StringBuffer newName;
            if (timeStr.isEmpty())
            {
                CDateTime now;
                now.setNow();
                now.getString(timeStr);
            }
            newName.append(archivedFile).append(".").append(timeStr);
            dstFile->move(newName);
            printf("Old %s is moved to %s.\n", archivedFile.str(), newName.str());
        }
        srcfile->move(archivedFile);
        printf("%s is archived to %s.\n", ackedFile.str(), archivedFile.str());
    }
}

void CArchiveTankFileHelper::cleanAckedLogFilesForLogAgentGroup(const char* groupName, StringArray& ackedLogFiles)
{
    Owned<IClientCleanAckedFilesRequest> req = client->createCleanAckedFilesRequest();
    req->setGroupName(groupName);
    req->setFileNames(ackedLogFiles);

    Owned<IClientCleanAckedFilesResponse> resp = client->CleanAckedFiles(req);
    if (!resp)
        throw makeStringException(-1, "Failed in CleanAckedFiles.");

    const IMultiException* excep = &resp->getExceptions();
    if (excep != nullptr && excep->ordinality() > 0)
    {
        StringBuffer msg;
        printf("%s\n", excep->errorMessage(msg).str());
    }
}

IClientWSDecoupledLog* createLogServerClient(IProperties* input)
{
    Owned<IClientWSDecoupledLog> client = createWSDecoupledLogClient();

    const char* server = input->queryProp("server");
    if (isEmptyString(server))
        throw MakeStringException(0, "Server url not defined");

    StringBuffer url(server);
    addPathSepChar(url);
    url.append("WSDecoupledLog");
    client->addServiceUrl(url.str());
    const char* user = input->queryProp("user");
    const char* password = input->queryProp("password");
    if (!isEmptyString(user))
        client->setUsernameToken(user, password, nullptr);

    return client.getClear();
}

void archiveTankFiles(IProperties* input)
{
    try
    {
        printf("Starting archiveTankFiles\n");
        Owned<CArchiveTankFileHelper> helper = new CArchiveTankFileHelper(input);
        helper->archive();
        printf("Finished archiveTankFiles\n");
    }
    catch (IException *e)
    {
        EXCLOG(e);
        e->Release();
    }
}
