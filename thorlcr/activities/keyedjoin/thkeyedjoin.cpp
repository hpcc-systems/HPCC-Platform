/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#include "dasess.hpp"
#include "dadfs.hpp"
#include "thexception.hpp"

#include "../hashdistrib/thhashdistrib.ipp"
#include "thkeyedjoin.ipp"
#include "jhtree.hpp"

class CKeyedJoinMaster : public CMasterActivity
{
    IHThorKeyedJoinArg *helper;
    Owned<CSlavePartMapping> dataFileMapping;
    Owned<IDistributedFile> indexFile, dataFile;
    MemoryBuffer offsetMapMb, initMb;
    bool localKey;
    unsigned numTags;
    mptag_t tags[4];
    ProgressInfoArray progressInfoArr;
    StringArray progressLabels;


public:
    CKeyedJoinMaster(CMasterGraphElement *info) : CMasterActivity(info)
    {
        helper = (IHThorKeyedJoinArg *) queryHelper();
        progressLabels.append("seeks");
        progressLabels.append("scans");
        progressLabels.append("accepted");
        progressLabels.append("postfiltered");
        progressLabels.append("prefiltered");

        if (helper->diskAccessRequired())
        {
            progressLabels.append("diskSeeks");
            progressLabels.append("diskAccepted");
            progressLabels.append("diskRejected");
        }
        ForEachItemIn(l, progressLabels)
            progressInfoArr.append(*new ProgressInfo);
        localKey = false;
        numTags = 0;
        tags[0] = tags[1] = tags[2] = tags[3] = TAG_NULL;
        reInit = 0 != (helper->getFetchFlags() & (FFvarfilename|FFdynamicfilename));
    }
    ~CKeyedJoinMaster()
    {
        unsigned i;
        for (i=0; i<4; i++)
            if (TAG_NULL != tags[i])
                container.queryJob().freeMPTag(tags[i]);
    }
    virtual void init()
    {
        OwnedRoxieString indexFileName(helper->getIndexFileName());
        indexFile.setown(queryThorFileManager().lookup(container.queryJob(), indexFileName, false, 0 != (helper->getJoinFlags() & JFindexoptional), true));

        unsigned keyReadWidth = (unsigned)container.queryJob().getWorkUnitValueInt("KJKRR", 0);
        if (!keyReadWidth || keyReadWidth>container.queryJob().querySlaves())
            keyReadWidth = container.queryJob().querySlaves();
        

        initMb.clear();
        initMb.append(indexFileName.get());
        if (helper->diskAccessRequired())
            numTags += 2;
        initMb.append(numTags);
        unsigned t=0;
        for (; t<numTags; t++)
        {
            tags[t] = container.queryJob().allocateMPTag();
            initMb.append(tags[t]);
        }
        bool keyHasTlk = false;
        if (indexFile)
        {
            unsigned numParts = 0;
            localKey = indexFile->queryAttributes().getPropBool("@local");

            if (container.queryLocalData() && !localKey)
                throw MakeActivityException(this, 0, "Keyed Join cannot be LOCAL unless supplied index is local");

            checkFormatCrc(this, indexFile, helper->getIndexFormatCrc(), true);
            Owned<IFileDescriptor> indexFileDesc = indexFile->getFileDescriptor();
            IDistributedSuperFile *superIndex = indexFile->querySuperFile();
            unsigned superIndexWidth = 0;
            unsigned numSuperIndexSubs = 0;
            if (superIndex)
            {
                numSuperIndexSubs = superIndex->numSubFiles(true);
                bool first=true;
                // consistency check
                Owned<IDistributedFileIterator> iter = superIndex->getSubFileIterator(true);
                ForEach(*iter)
                {
                    IDistributedFile &f = iter->query();
                    unsigned np = f.numParts()-1;
                    IDistributedFilePart &part = f.queryPart(np);
                    const char *kind = part.queryAttributes().queryProp("@kind");
                    bool hasTlk = NULL != kind && 0 == stricmp("topLevelKey", kind); // if last part not tlk, then deemed local (might be singlePartKey)
                    if (first)
                    {
                        first = false;
                        keyHasTlk = hasTlk;
                        superIndexWidth = f.numParts();
                        if (keyHasTlk)
                            --superIndexWidth;
                    }
                    else
                    {
                        if (hasTlk != keyHasTlk)
                            throw MakeActivityException(this, 0, "Local/Single part keys cannot be mixed with distributed(tlk) keys in keyedjoin");
                        if (keyHasTlk && superIndexWidth != f.numParts()-1)
                            throw MakeActivityException(this, 0, "Super sub keys of different width cannot be mixed with distributed(tlk) keys in keyedjoin");
                    }
                }
                if (keyHasTlk)
                    numParts = superIndexWidth * numSuperIndexSubs;
                else
                    numParts = superIndex->numParts();
            }
            else
            {
                numParts = indexFile->numParts();
                if (numParts)
                {
                    const char *kind = indexFile->queryPart(indexFile->numParts()-1).queryAttributes().queryProp("@kind");
                    keyHasTlk = NULL != kind && 0 == stricmp("topLevelKey", kind);
                    if (keyHasTlk)
                        --numParts;
                }
            }
            if (numParts)
            {
                initMb.append(numParts);
                initMb.append(superIndexWidth); // 0 if not superIndex
                initMb.append((superIndex && superIndex->isInterleaved()) ? numSuperIndexSubs : 0);
                unsigned p=0;
                UnsignedArray parts;
                for (; p<numParts; p++)
                    parts.append(p);
                indexFileDesc->serializeParts(initMb, parts);
                if (localKey)
                    keyHasTlk = false; // not used
                initMb.append(keyHasTlk);
                if (keyHasTlk)
                {
                    if (numSuperIndexSubs)
                        initMb.append(numSuperIndexSubs);
                    else
                        initMb.append((unsigned)1);

                    Owned<IDistributedFileIterator> iter;
                    IDistributedFile *f;
                    if (superIndex)
                    {
                        iter.setown(superIndex->getSubFileIterator(true));
                        f = &iter->query();
                    }
                    else
                        f = indexFile;
                    loop
                    {
                        unsigned location;
                        OwnedIFile iFile;
                        StringBuffer filePath;
                        Owned<IFileDescriptor> fileDesc = f->getFileDescriptor();
                        Owned<IPartDescriptor> tlkDesc = fileDesc->getPart(fileDesc->numParts()-1);
                        if (!getBestFilePart(this, *tlkDesc, iFile, location, filePath))
                            throw MakeThorException(TE_FileNotFound, "Top level key part does not exist, for key: %s", f->queryLogicalName());
                        OwnedIFileIO iFileIO = iFile->open(IFOread);
                        assertex(iFileIO);

                        size32_t tlkSz = (size32_t)iFileIO->size();
                        initMb.append(tlkSz);
                        ::read(iFileIO, 0, tlkSz, initMb);

                        if (!iter || !iter->next())
                            break;
                        f = &iter->query();
                    }
                }
                if (helper->diskAccessRequired())
                {
                    OwnedRoxieString fetchFilename(helper->getFileName());
                    if (fetchFilename)
                    {
                        dataFile.setown(queryThorFileManager().lookup(container.queryJob(), fetchFilename, false, 0 != (helper->getFetchFlags() & FFdatafileoptional), true));
                        if (dataFile)
                        {
                            if (superIndex)
                                throw MakeActivityException(this, 0, "Superkeys and full keyed joins are not supported");
                            Owned<IFileDescriptor> dataFileDesc = getConfiguredFileDescriptor(*dataFile);
                            void *ekey;
                            size32_t ekeylen;
                            helper->getFileEncryptKey(ekeylen,ekey);
                            bool encrypted = dataFileDesc->queryProperties().getPropBool("@encrypted");
                            if (0 != ekeylen)
                            {
                                memset(ekey,0,ekeylen);
                                free(ekey);
                                if (!encrypted)
                                {
                                    Owned<IException> e = MakeActivityWarning(&container, TE_EncryptionMismatch, "Ignoring encryption key provided as file '%s' was not published as encrypted", helper->getFileName());
                                    container.queryJob().fireException(e);
                                }
                            }
                            else if (encrypted)
                                throw MakeActivityException(this, 0, "File '%s' was published as encrypted but no encryption key provided", fetchFilename.get());
                            unsigned dataReadWidth = (unsigned)container.queryJob().getWorkUnitValueInt("KJDRR", 0);
                            if (!dataReadWidth || dataReadWidth>container.queryJob().querySlaves())
                                dataReadWidth = container.queryJob().querySlaves();
                            Owned<IGroup> grp = container.queryJob().querySlaveGroup().subset((unsigned)0, dataReadWidth);
                            dataFileMapping.setown(getFileSlaveMaps(dataFile->queryLogicalName(), *dataFileDesc, container.queryJob().queryUserDescriptor(), *grp, false, false, NULL));
                            dataFileMapping->serializeFileOffsetMap(offsetMapMb.clear());
                            queryThorFileManager().noteFileRead(container.queryJob(), dataFile);
                        }
                        else
                            indexFile.clear();
                    }
                }
            }
            else
                indexFile.clear();
        }
        if (indexFile)
            queryThorFileManager().noteFileRead(container.queryJob(), indexFile);
        else
            initMb.append((unsigned)0);
    }
    virtual void serializeSlaveData(MemoryBuffer &dst, unsigned slave)
    {
        dst.append(initMb);
        if (indexFile && helper->diskAccessRequired())
        {
            if (dataFile)
            {
                dataFileMapping->serializeMap(slave, dst);
                dst.append(offsetMapMb);
            }
            else
            {
                CSlavePartMapping::serializeNullMap(dst);
                CSlavePartMapping::serializeNullOffsetMap(dst);
            }
        }
    }
    virtual void deserializeStats(unsigned node, MemoryBuffer &mb)
    {
        CMasterActivity::deserializeStats(node, mb);
        ForEachItemIn(p, progressLabels)
        {
            unsigned __int64 st;
            mb.read(st);
            progressInfoArr.item(p).set(node, st);
        }
    }
    virtual void getXGMML(unsigned idx, IPropertyTree *edge)
    {
        CMasterActivity::getXGMML(idx, edge);
        assertex(0 == idx);
        ForEachItemIn(p, progressInfoArr)
        {
            ProgressInfo &progress = progressInfoArr.item(p);
            progress.processInfo();
            StringBuffer attr("@");
            attr.append(progressLabels.item(p));
            edge->setPropInt64(attr.str(), progress.queryTotal());
        }
    }
    virtual void kill()
    {
        CMasterActivity::kill();
        indexFile.clear();
        dataFile.clear();
    }
    virtual void done()
    {
        CMasterActivity::done();
        if (indexFile)
            indexFile->setAccessed();
        if (dataFile)
            dataFile->setAccessed();
    }
};


CActivityBase *createKeyedJoinActivityMaster(CMasterGraphElement *info)
{
    return new CKeyedJoinMaster(info);
}
