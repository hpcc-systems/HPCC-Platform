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

#include "jliball.hpp"
#include "platform.h"
#include "jlib.hpp"
#include "jio.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jsocket.hpp"
#include "jdebug.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "rmtspawn.hpp"
#include "filecopy.hpp"
#include "fttransform.ipp"
#include "daftformat.hpp"
#include "daftdir.hpp"
#include "daftsize.hpp"
#include "daftcfg.hpp"
#include "mptag.hpp"

bool processPullCommand(ISocket * masterSocket, MemoryBuffer & msg)
{
    srand((int)get_cycles_now());
    TransferServer server(masterSocket);
    server.deserializeAction(msg, FTactionpull);
    return server.pull();
}


bool processPushCommand(ISocket * masterSocket, MemoryBuffer & msg)
{
    srand((int)get_cycles_now());
    TransferServer server(masterSocket);
    server.deserializeAction(msg, FTactionpush);
    return server.push();
}


bool processPartitionCommand(ISocket * masterSocket, MemoryBuffer & msg, MemoryBuffer & results)
{
    FileFormat srcFormat;
    FileFormat tgtFormat;
    unsigned whichInput;
    RemoteFilename fullPath;
    offset_t totalSize;
    offset_t thisOffset;
    offset_t thisSize;
    unsigned thisHeaderSize;
    unsigned numParts;
    bool compressedInput = false;
    unsigned compatflags = 0;  

    srcFormat.deserialize(msg);
    tgtFormat.deserialize(msg);
    msg.read(whichInput);
    fullPath.deserialize(msg);
    msg.read(totalSize);
    msg.read(thisOffset);
    msg.read(thisSize);
    msg.read(thisHeaderSize);
    msg.read(numParts);
    if (msg.remaining())
        msg.read(compressedInput);
    if (msg.remaining())
        msg.read(compatflags); // not yet used
    StringAttr decryptkey;
    if (msg.remaining())
        msg.read(decryptkey);
    if (msg.remaining())
    {
        srcFormat.deserializeExtra(msg, 1);
        tgtFormat.deserializeExtra(msg, 1);
    }

    StringBuffer text;
    fullPath.getRemotePath(text);
    LOG(MCdebugProgress, unknownJob, "Process partition %d(%s)", whichInput, text.str());
    Owned<IFormatProcessor> processor = createFormatProcessor(srcFormat, tgtFormat, true);
    Owned<IOutputProcessor> target = createOutputProcessor(tgtFormat);
    processor->setTarget(target);

    processor->setPartitionRange(totalSize, thisOffset, thisSize, thisHeaderSize, numParts);
    processor->setSource(whichInput, fullPath, compressedInput, decryptkey);
    processor->calcPartitions(NULL);

    PartitionPointArray partition;
    processor->getResults(partition);

    serialize(partition, results);
    return true;
}


class FtSlave : public CRemoteSlave
{
public:
    FtSlave() : CRemoteSlave("ftslave", MPTAG_FT_SLAVE, DAFT_VERSION, false) {}

    virtual bool processCommand(byte action, ISocket * masterSocket, MemoryBuffer & msg, MemoryBuffer & results)
    {
        switch (action)
        {
        case FTactionpartition:
            return processPartitionCommand(masterSocket, msg, results);
        case FTactionpull:
            return processPullCommand(masterSocket, msg);
        case FTactionpush:
            return processPushCommand(masterSocket, msg);
        case FTactiondirectory:
            return processDirCommand(masterSocket, msg, results);
        case FTactionsize:
            return processSizesCommand(masterSocket, msg, results);
        case FTactionpcopy:
            return processPhysicalCopyCommand(masterSocket, msg, results);
        default:
            UNIMPLEMENTED;
        }
        return false;
    }
};


int main(int argc, char * argv[])
{
    InitModuleObjects();
    setDaliServixSocketCaching(true);
    installDefaultFileHooks(nullptr);
    FtSlave slave;
    slave.run(argc, argv);
    return 0;
}
