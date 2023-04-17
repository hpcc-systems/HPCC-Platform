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

#pragma warning (disable : 4786)
#pragma warning (disable : 4129)

#include "jiface.hpp"
#include "environment.hpp"
#include "dautils.hpp"
#include "ws_fileioservice.hpp"
#ifdef _WIN32
#include "windows.h"
#endif
#include "exception_util.hpp"

#define FILE_IO_URL     "FileIOAccess"

void CWsFileIOEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    CTpWrapper tpWrapper;
    tpWrapper.getTpDropZones(9999, nullptr, false, allTpDropZones); //version 9999: get the latest information about dropzone
}

bool CWsFileIOEx::onCreateFile(IEspContext &context, IEspCreateFileRequest &req, IEspCreateFileResponse &resp)
{
    context.ensureFeatureAccess(FILE_IO_URL, SecAccess_Write, ECLWATCH_ACCESS_TO_FILE_DENIED, "WsFileIO::CreateFile: Permission denied");

    StringBuffer result;
    const char* server = req.getDestDropZone();
    if (!server || (server[0] == 0))
    {
        resp.setResult("Destination not specified");
        return true;
    }

    const char* destRelativePath = req.getDestRelativePath();
    if (!destRelativePath || (destRelativePath[0] == 0))
    {
        resp.setResult("Destination path not specified");
        return true;
    }

    resp.setDestDropZone(server);
    resp.setDestRelativePath(destRelativePath);

    CDfsLogicalFileName dlfn;
    validateDropZoneAccess(context, server, req.getDestNetAddress(), SecAccess_Write, destRelativePath, dlfn);

    RemoteFilename rfn;
    dlfn.getExternalFilename(rfn);
    Owned<IFile> file = createIFile(rfn);

    fileBool isDir = file->isDirectory();
    if (isDir == fileBool::foundYes)
    {
        result.appendf("Failure: %s is a directory.", destRelativePath);
        resp.setResult(result.str());
        return true;
    }

    if (!req.getOverwrite() && (isDir != fileBool::notFound))
    {
        result.appendf("Failure: %s exists.", destRelativePath);
        resp.setResult(result.str());
        return true;
    }

    file->open(IFOcreate);
    result.appendf("%s has been created.", destRelativePath);
    resp.setResult(result.str());
    return true;
}

bool CWsFileIOEx::onReadFileData(IEspContext &context, IEspReadFileDataRequest &req, IEspReadFileDataResponse &resp)
{
    context.ensureFeatureAccess(FILE_IO_URL, SecAccess_Read, ECLWATCH_ACCESS_TO_FILE_DENIED, "WsFileIO::ReadFileData: Permission denied");

    StringBuffer result;
    const char* server = req.getDestDropZone();
    if (!server || (server[0] == 0))
    {
        resp.setResult("Destination not specified");
        return true;
    }

    const char* destRelativePath = req.getDestRelativePath();
    if (!destRelativePath || (destRelativePath[0] == 0))
    {
        resp.setResult("Destination path not specified");
        return true;
    }

    resp.setDestDropZone(server);
    resp.setDestRelativePath(destRelativePath);

    __int64 dataSize = req.getDataSize();
    __int64 offset = req.getOffset();
    resp.setDataSize(dataSize);
    resp.setOffset(offset);
    if (dataSize < 1)
    {
        resp.setResult("Invalid data size.");
        return true;
    }

    if (offset < 0)
    {
        resp.setResult("Invalid offset.");
        return true;
    }

    CDfsLogicalFileName dlfn;
    validateDropZoneAccess(context, server, req.getDestNetAddress(), SecAccess_Read, destRelativePath, dlfn);

    StringBuffer user;
    RemoteFilename rfn;
    dlfn.getExternalFilename(rfn);
    Owned<IFile> file = createIFile(rfn);
    fileBool isFile = file->isFile();
    if (isFile != fileBool::foundYes)
    {
        result.appendf("%s does not exist.", destRelativePath);
        resp.setResult(result.str());
        return true;
    }

    Owned<IFileIO> io = file->open(IFOread);
    __int64 size = io->size();
    if (offset >= size)
    {
        result.appendf("Invalid offset: file size = %" I64F "d.", size);
        resp.setResult(result.str());
        return true;
    }

    __int64 dataToRead = size - offset;
    if (dataToRead > dataSize)
    {
        dataToRead = dataSize;
    }

    MemoryBuffer membuf;
    char* buf = (char*) membuf.reserve((int) dataToRead);
    if (io->read(offset, (int)dataToRead, buf) != dataToRead)
    {
        resp.setResult("ReadFileData error.");
        LOG(MCprogress, unknownJob, "ReadFileData error: %s: %s %s", context.getUserID(user).str(), server, destRelativePath);
    }
    else
    {
        resp.setData(membuf);
        resp.setResult("ReadFileData done.");
        LOG(MCprogress, unknownJob, "ReadFileData done: %s: %s %s", context.getUserID(user).str(), server, destRelativePath);
    }

    return true;
}

bool CWsFileIOEx::onWriteFileData(IEspContext &context, IEspWriteFileDataRequest &req, IEspWriteFileDataResponse &resp)
{
    context.ensureFeatureAccess(FILE_IO_URL, SecAccess_Write, ECLWATCH_ACCESS_TO_FILE_DENIED, "WsFileIO::WriteFileData: Permission denied");

    StringBuffer result;
    const char* server = req.getDestDropZone();
    if (!server || (server[0] == 0))
    {
        resp.setResult("Destination not specified");
        return true;
    }

    const char* destRelativePath = req.getDestRelativePath();
    if (!destRelativePath || (destRelativePath[0] == 0))
    {
        resp.setResult("Destination path not specified");
        return true;
    }

    MemoryBuffer& srcdata = (MemoryBuffer&)req.getData();
    if(srcdata.length() == 0)
    {
        resp.setResult("Source data not specified");
        return true;
    }

    __int64 offset = req.getOffset();
    bool append = req.getAppend();  
    if (append)
    {
        resp.setAppend(true);
    }
    else
    {
        resp.setOffset(offset);
    }

    resp.setDestDropZone(server);
    resp.setDestRelativePath(destRelativePath);

    CDfsLogicalFileName dlfn;
    validateDropZoneAccess(context, server, req.getDestNetAddress(), SecAccess_Write, destRelativePath, dlfn);

    StringBuffer user;
    RemoteFilename rfn;
    dlfn.getExternalFilename(rfn);
    Owned<IFile> file = createIFile(rfn);
    fileBool isFile = file->isFile();
    if (isFile != fileBool::foundYes)
    {
        result.appendf("%s does not exist.", destRelativePath);
        resp.setResult(result.str());
        return true;
    }

    if (append)
    {
        Owned<IFileIO> io = file->open(IFOread);
        offset = io->size();
    }

    Owned<IFileIO> fileio = file->open(IFOwrite);
    size32_t len = srcdata.length();
    if (fileio->write(offset, len, srcdata.readDirect(len)) != len)
    {
        resp.setResult("WriteFileData error.");
        LOG(MCprogress, unknownJob, "WriteFileData error: %s: %s %s", context.getUserID(user).str(), server, destRelativePath);
    }
    else
    {
        resp.setResult("WriteFileData done.");
        LOG(MCprogress, unknownJob, "WriteFileData done: %s: %s %s", context.getUserID(user).str(), server, destRelativePath);
    }

    return true;
}


