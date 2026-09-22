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

#include "dadfs.hpp"

#include "thorport.hpp"
#include "thexception.hpp"
#include "thmfilemanager.hpp"
#include "thdiskbase.ipp"
#include "thcsvread.ipp"

class CCsvReadActivityMaster : public CDiskReadMasterBase
{
    IHThorCsvReadArg *helper;
    unsigned headerLines;
    UnsignedArray firstNonEmptyPart; // per subfile (index 0 if not a superfile): local part index that owns the header
    bool firstNonEmptyPartCalculated = false;

    /* Workers only ever see the parts assigned to them, so a worker cannot reliably determine, on its own,
     * which part of a subfile is the first non-empty one - different parts of the same subfile can be
     * processed by different workers, in any order. The master has the full file descriptor, so it
     * determines this once, up-front, and conveys the result to every worker.
     */
    void calcFirstNonEmptyPart(unsigned subFiles)
    {
        for (unsigned s=0; s<subFiles; s++)
            firstNonEmptyPart.append(0); // default/fallback: assume local part 0 owns the header
        if (!fileDesc)
            return;
        ISuperFileDescriptor *superFDesc = fileDesc->querySuperFileDescriptor();
        BoolArray found;
        for (unsigned s=0; s<subFiles; s++)
            found.append(false);
        unsigned numParts = fileDesc->numParts();
        for (unsigned p=0; p<numParts; p++)
        {
            unsigned subFile = 0, localIdx = p;
            if (superFDesc && !superFDesc->mapSubPart(p, subFile, localIdx))
                continue;
            if ((subFile >= subFiles) || found.item(subFile))
                continue;
            offset_t partSize = fileDesc->queryPart(p)->getFileSize(false, false);
            if (((offset_t)-1 != partSize) && (partSize > 0))
            {
                firstNonEmptyPart.replace(localIdx, subFile);
                found.replace(true, subFile);
            }
        }
        firstNonEmptyPartCalculated = true;
    }
public:
    CCsvReadActivityMaster(CMasterGraphElement *info) : CDiskReadMasterBase(info)
    {
        helper = (IHThorCsvReadArg *)queryHelper();
        headerLines = helper->queryCsvParameters()->queryHeaderLen();
        if (headerLines)
            mpTag = container.queryJob().allocateMPTag();
    }
    virtual void init() override
    {
        firstNonEmptyPart.kill();
        firstNonEmptyPartCalculated = false;
        CDiskReadMasterBase::init();
    }
    virtual void validateFile(IDistributedFile *file) override
    {
        // NB: CSV can be used to read any format
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave) override
    {
        CDiskReadMasterBase::serializeSlaveData(dst, slave);
        if (mapping && mapping->queryMapWidth(slave)>=1)
        {
            if (fileDesc->queryProperties().hasProp("@csvQuote")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvQuote"));
            else dst.append(false);
            if (fileDesc->queryProperties().hasProp("@csvSeparate")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvSeparate"));
            else dst.append(false);
            if (fileDesc->queryProperties().hasProp("@csvTerminate")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvTerminate"));
            else dst.append(false);
            if (fileDesc->queryProperties().hasProp("@csvEscape")) dst.append(true).append(fileDesc->queryProperties().queryProp("@csvEscape"));
            else dst.append(false);
        }
        if (headerLines)
        {
            dst.append((int)mpTag);
            unsigned subFiles = 0;
            if (fileDesc)
            {
                ISuperFileDescriptor *superFDesc = fileDesc->querySuperFileDescriptor();
                subFiles = superFDesc ? superFDesc->querySubFiles() : 1;
            }
            dst.append(subFiles);
            if (!firstNonEmptyPartCalculated)
                calcFirstNonEmptyPart(subFiles);
            for (unsigned s=0; s<subFiles; s++)
                dst.append(firstNonEmptyPart.item(s));
        }
    }
};


CActivityBase *createCCsvReadActivityMaster(CMasterGraphElement *container)
{
    return new CCsvReadActivityMaster(container);
}

