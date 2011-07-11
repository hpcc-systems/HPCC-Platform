/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/dali/ft/ftslave.cpp $ $Id: ftslave.cpp 64048 2011-04-15 12:44:57Z nhicks $");

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
    StringAttr encryptkey;
    if (msg.remaining())
        msg.read(encryptkey);
    StringBuffer text;
    fullPath.getRemotePath(text);
    LOG(MCdebugProgress, unknownJob, "Process partition %d(%s)", whichInput, text.str());
    Owned<IFormatProcessor> processor = createFormatProcessor(srcFormat, tgtFormat, true);
    Owned<IOutputProcessor> target = createOutputProcessor(tgtFormat);
    processor->setTarget(target);

    processor->setPartitionRange(totalSize, thisOffset, thisSize, thisHeaderSize, numParts);
    processor->setSource(whichInput, fullPath, compressedInput, encryptkey);
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
    FtSlave slave;
    slave.run(argc, argv);
    return 0;
}
