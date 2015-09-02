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

#include "platform.h"
#include "jlzw.hpp"
#include "jlzma.hpp"
#include "jexcept.hpp"
#include "jhtree.hpp"
#include "ctfile.hpp"
#include "keybuild.hpp"
#include "limits.h"
#include "keydiff.hpp"

#define SMALL_ENOUGH_RATIO 20

#define KEYDIFFSIG    "KEYDIFF"
#define LZMA_FLAG  (0x80000000)

class RowBuffer
{
public:
    RowBuffer() : rowsize(0), thisrowsize(0), buffsize(0), buffer(0), fpos(0) {}

    ~RowBuffer() { free(buffer); }

    void init(size32_t _rowsize, bool _isVar)
    {
        rowsize = _rowsize;
        isVar = _isVar;
        thisrowsize = rowsize;
        buffsize = rowsize + sizeof(offset_t);
        buffer = calloc(buffsize, 1);
        fpos = reinterpret_cast<offset_t *>(buffer);
        row = reinterpret_cast<char *>(buffer) + sizeof(offset_t);
        *fpos = 0;
    }

    void clear()
    {
        memset(row, 0, rowsize);
        *fpos = 0;
    }

    bool getCursorNext(IKeyCursor * keyCursor)
    {
        if(keyCursor->next(row))
        {
            if(isVar)
                thisrowsize = keyCursor->getSize();
            *fpos = keyCursor->getFPos();
            return true;
        }
        *fpos = 0;
        return false;
    }

    void putBuilder(IKeyBuilder * keyBuilder, unsigned __int64 reccount)
    {
        keyBuilder->processKeyData(row, *fpos, thisrowsize);
    }

    offset_t queryFPos() const { return *fpos; }

    void setFPos(offset_t fp) { *fpos = fp; }

    CNodeInfo * getNodeInfo(unsigned __int64 reccount)
    {
        return new CNodeInfo(*fpos, row, thisrowsize, reccount);
    }

    void tally(CRC32 & crc) { crc.tally(sizeof(offset_t) + thisrowsize, buffer); }

    int compareKeyed(RowBuffer const & other) const
    {
        size32_t minsize = thisrowsize;
        if(other.thisrowsize < minsize)
            minsize = other.thisrowsize;
        int cmp = memcmp(row, other.row, minsize);
        if(cmp != 0)
            return cmp;
        if(thisrowsize < other.thisrowsize)
            return -1;
        if(thisrowsize > other.thisrowsize)
            return +1;
        return 0;
    }

    void swap(RowBuffer & other)
    {
        void * tmpb = other.buffer;
        char * tmpr = other.row;
        offset_t * tmpf = other.fpos;
        other.buffer = buffer;
        other.row = row;
        other.fpos = fpos;
        buffer = tmpb;
        row = tmpr;
        fpos = tmpf;
        if(isVar)
        {
            size32_t tmps = other.thisrowsize;
            other.thisrowsize = thisrowsize;
            thisrowsize = tmps;
        }
    }

    size32_t diffCompress(RowBuffer const & prev, char * dst) const
    {
        if(isVar)
        {
            size32_t maxrowsize;
            if(thisrowsize > prev.thisrowsize)
            {
                maxrowsize = thisrowsize;
                memset(prev.row + prev.thisrowsize, 0, thisrowsize - prev.thisrowsize);
            }
            else
            {
                maxrowsize = prev.thisrowsize;
                if(prev.thisrowsize > thisrowsize)
                    memset(row + thisrowsize, 0, prev.thisrowsize - thisrowsize);
            }
            *reinterpret_cast<size32_t *>(dst) = thisrowsize;
            return (sizeof(size32_t) + DiffCompress2(buffer, dst + sizeof(size32_t), prev.buffer, maxrowsize + sizeof(offset_t)));
        }
        else
        {
            return DiffCompress2(buffer, dst, prev.buffer, buffsize);
        }
    }

    size32_t diffExpand(byte const * src, RowBuffer const & prev)
    {
        if(isVar)
        {
            thisrowsize = *reinterpret_cast<size32_t const *>(src);
            unsigned maxrowsize;
            if(thisrowsize > prev.thisrowsize)
            {
                maxrowsize = thisrowsize;
                memset(prev.row + prev.thisrowsize, 0, thisrowsize - prev.thisrowsize);
            }
            else
                maxrowsize = prev.thisrowsize;
            return (sizeof(size32_t) + DiffExpand(src + sizeof(size32_t), buffer, prev.buffer, maxrowsize + sizeof(offset_t)));
        }
        else
        {
            return DiffExpand(src, buffer, prev.buffer, buffsize);
        }
    }

    void copyFrom(RowBuffer const & src)
    {
        memcpy(buffer, src.buffer, buffsize);
        if(isVar)
            thisrowsize = src.thisrowsize;
    }

    size32_t buffSize() { return buffsize; }
    size32_t rowSize() { return thisrowsize; }
    size32_t serializeRowSize() { return thisrowsize+sizeof(offset_t)+(isVar?sizeof(size32_t):0); }

    size32_t serialize(void *dst)
    {
        size32_t ret = thisrowsize+sizeof(offset_t);
        if (isVar) {
            *((size32_t *)dst) = ret;
            memcpy((byte *)dst+sizeof(size32_t),buffer,ret);
            ret += sizeof(size32_t);
        }
        else
            memcpy((byte *)dst,buffer,ret);
        return ret;
    }

    size32_t deserialize(const void *src)
    {
        size32_t ret = 0;
        if (isVar) {
            thisrowsize = *(const size32_t *)src;
            thisrowsize -= sizeof(offset_t);
            src = (const byte *)src + sizeof(size32_t);
            ret += sizeof(size32_t);
        }
        size32_t cp = thisrowsize+sizeof(offset_t);
        assertex(buffsize>=cp);
        memcpy(buffer,src,cp);
        return ret+cp;
    }

private:
    size32_t rowsize;
    bool isVar;
    size32_t thisrowsize;
    size32_t buffsize;
    void * buffer;
    char * row;
    offset_t * fpos;
};

class CKeyReader: public CInterface
{
public:
    CKeyReader(char const * filename) : count(0)
    {
        keyFile.setown(createIFile(filename));
        keyFileIO.setown(keyFile->open(IFOread));
        if(!keyFileIO)
            throw MakeStringException(0, "Could not read index file %s", filename);
        keyIndex.setown(createKeyIndex(filename, 0, *keyFileIO, false, false)); // MORE - should we care about crc?
        unsigned flags = keyIndex->getFlags();
        variableWidth = ((flags & HTREE_VARSIZE) == HTREE_VARSIZE);
        if((flags & HTREE_QUICK_COMPRESSED_KEY) == HTREE_QUICK_COMPRESSED_KEY)
            quickCompressed = true;
        else if((flags & HTREE_COMPRESSED_KEY) == HTREE_COMPRESSED_KEY)
            quickCompressed = false;
        else
            throw MakeStringException(0, "Index file %s did not have compression flags set, unsupported", filename);
        unsigned optionalFlags = (HTREE_VARSIZE | HTREE_QUICK_COMPRESSED_KEY | HTREE_TOPLEVEL_KEY | HTREE_FULLSORT_KEY);
        unsigned requiredFlags = COL_PREFIX;
#ifdef _DEBUG
        if((flags & ~optionalFlags) != requiredFlags)
            ERRLOG("Index file %s did not have expected index flags set (%x)", filename, (flags & ~optionalFlags) );
#else
        if((flags & ~optionalFlags) != requiredFlags)
            throw MakeStringException(0, "Index file %s did not have expected index flags set (%x)", filename, (flags & ~optionalFlags) );
#endif
        offset_t blobHead = keyIndex->queryBlobHead();
        if(blobHead == static_cast<offset_t>(-1))
            WARNLOG("Index part %s does not declare blob status: if it contains blobs, they will be lost", filename);
        else if(blobHead != 0)
            throw MakeStringException(0, "Index contains BLOBs, which are currently not supported by keydiff/patch");
        if(keyIndex->queryMetadataHead())
            WARNLOG("Index contains metadata, which will be ignored by keydiff/patch");
        keyCursor.setown(keyIndex->getCursor(NULL));
        if(keyIndex->hasPayload())
            keyedsize = keyIndex->keyedSize();
        else
            keyedsize = static_cast<unsigned>(-1);
        rowsize = keyIndex->keySize();
        eof = false;
    }

    bool get(RowBuffer & buffer)
    {
        if(eof)
            return false;
        if(buffer.getCursorNext(keyCursor))
        {
            buffer.tally(crc);
            count++;
            checkProgress();
            return true;
        }
        eof = true;
        checkProgress();
        return false;
    }

    void getToEnd(RowBuffer & buffer)
    {
        while(!eof)
            get(buffer);
    }

    void getRawToEnd()
    {
        char * buff = reinterpret_cast<char *>(malloc(rowsize));
        while(!eof)
        {
            if(keyCursor->next(buff))
            {
                offset_t fpos = keyCursor->getFPos();
                crc.tally(rowsize, buff);
                crc.tally(sizeof(fpos), &fpos);
            }
            else
                eof = true;
        }
        free(buff);
    }

    size32_t queryKeyedSize() const { return keyedsize; }
    size32_t queryRowSize() const { return rowsize; }
    unsigned queryCRC() { return crc.get(); }
    unsigned queryCount() const { return count; }
    bool isVariableWidth() const { return variableWidth; }
    bool isQuickCompressed() const { return quickCompressed; }
    unsigned getNodeSize() const { return keyIndex->getNodeSize(); }

    virtual void setProgressCallback(IKeyDiffProgressCallback * callback, offset_t freq)
    {
        progressCallback.setown(callback);
        progressFrequency = freq;
        progressCount = 0;
    }

private:
    void checkProgress()
    {
        if(!progressCallback)
            return;
        offset_t latest = keyIndex->queryLatestGetNodeOffset();
        if((latest - progressCount) < progressFrequency)
            return;
        do
        {
            progressCount += progressFrequency;
        } while((latest - progressCount) >= progressFrequency);
        progressCallback->handle(latest);
    }
    
private:
    Owned<IFile> keyFile;
    Owned<IFileIO> keyFileIO;
    Owned<IKeyIndex> keyIndex;
    Owned<IKeyCursor> keyCursor;
    CRC32 crc;
    size32_t keyedsize;
    size32_t rowsize;
    bool eof;
    unsigned count;
    bool variableWidth;
    bool quickCompressed;
    Owned<IKeyDiffProgressCallback> progressCallback;
    offset_t progressFrequency;
    offset_t progressCount;
};

class CKeyFileReader: public CInterface, extends IKeyFileRowReader
{
    CKeyReader reader;
    Owned<IPropertyTree> header;
    RowBuffer buffer;

public:
    IMPLEMENT_IINTERFACE;
    CKeyFileReader(const char *filename)
        : reader(filename)
    {
        size32_t rowsize = reader.queryRowSize();
        bool isvar = reader.isVariableWidth();
        buffer.init(rowsize,isvar);

        header.setown(createPTree("Index"));
        header->setPropInt("@rowSize",rowsize);
        header->setPropInt("@keyedSize",reader.queryKeyedSize());
        header->setPropBool("@variableWidth",isvar);
        header->setPropBool("@quickCompressed",reader.isQuickCompressed());
#if 0
        PROGLOG("rowSize = %d",rowsize);
        PROGLOG("keyedSize = %d",reader.queryKeyedSize());
        PROGLOG("variableWidth = %s",isvar?"true":"false");
        PROGLOG("quickCompressed = %s",reader.isQuickCompressed()?"true":"false");
#endif
        
    }

    const void *nextRow()
    {
        if (!reader.get(buffer)) 
            return NULL;
        void *ret = malloc(buffer.serializeRowSize());
        buffer.serialize(ret);
        return ret;
    }       

    
    void stop()
    {
    }

    IPropertyTree *queryHeader()
    {
        return header;
    }
};



class CKeyWriter: public CInterface
{
public:
    CKeyWriter()
    {
    }
        
    void init (char const * filename, bool overwrite, size32_t _keyedsize, size32_t _rowsize, bool variableWidth, bool quickCompressed, unsigned nodeSize) 
    {
        keyedsize = _keyedsize;
        rowsize = _rowsize;
        reccount = 0;
        keyFile.setown(createIFile(filename));
        if(!overwrite && (keyFile->isFile() != notFound))
            throw MakeStringException(0, "Found preexisting index file %s (overwrite not selected)", filename);
        keyFileIO.setown(keyFile->openShared(IFOcreate, IFSHfull)); // not sure if needs shared here
        if(!keyFileIO)
            throw MakeStringException(0, "Could not write index file %s", filename);
        keyStream.setown(createIOStream(keyFileIO));
        unsigned flags = COL_PREFIX | HTREE_FULLSORT_KEY | HTREE_COMPRESSED_KEY;
        if(variableWidth)
            flags |= HTREE_VARSIZE;
        if(quickCompressed)
            flags |= HTREE_QUICK_COMPRESSED_KEY;
        keyBuilder.setown(createKeyBuilder(keyStream, flags, rowsize, nodeSize, keyedsize, 0)); // MORE - support for sequence other than 0...
    }

    ~CKeyWriter()
    {
        if (keyBuilder)
            keyBuilder->finish();
    }

    void put(RowBuffer & buffer)
    {
        buffer.tally(crc);
        buffer.putBuilder(keyBuilder, reccount++);
    }

    void putNode(CNodeInfo & info)
    {
        crc.tally(rowsize, info.value);
        crc.tally(sizeof(info.pos), &(info.pos));
        keyBuilder->processKeyData(reinterpret_cast<char *>(info.value), info.pos, info.size);
    }

    unsigned __int64 queryCount()
    {
        return reccount;
    }

    unsigned queryCRC()
    {
        return crc.get();
    }

    offset_t getPosition()
    {
        return keyStream->tell();
    }

private:
    Owned<IFile> keyFile;
    Owned<IFileIO> keyFileIO;
    Owned<IFileIOStream> keyStream;
    Owned<IKeyBuilder> keyBuilder;
    CRC32 crc;
    size32_t keyedsize;
    size32_t rowsize;
    unsigned __int64 reccount;
};

class KeyDiffVersion
{
public:
    KeyDiffVersion(unsigned short _mjr, unsigned short _mnr) : mjr(_mjr), mnr(_mnr) {}
    KeyDiffVersion(KeyDiffVersion const & other) : mjr(other.mjr), mnr(other.mnr) {}
    unsigned short queryMajor() const { return mjr; }
    unsigned short queryMinor() const { return mnr; }
    void serialize(MemoryBuffer & buff) const { buff.append(mjr).append(mnr); }
    void deserialize(MemoryBuffer & buff) { buff.read(mjr).read(mnr); }
    bool operator<(KeyDiffVersion const & other) const { return ((mjr < other.mjr) || ((mjr == other.mjr) && (mnr < other.mnr))); }
    static size32_t querySerializedSize() { return 2*sizeof(unsigned short); }
private:
    unsigned short mjr;
    unsigned short mnr;
};


class CKeyFileWriter: public CInterface, extends IKeyFileRowWriter
{
    CKeyWriter writer;
    Owned<IPropertyTree> header;
    RowBuffer buffer;


public:
    IMPLEMENT_IINTERFACE;
    CKeyFileWriter(const char *filename, IPropertyTree *_header, bool overwrite, unsigned nodeSize)
        : header(createPTreeFromIPT(_header))
    {
        writer.init(filename,overwrite,header->getPropInt("@keyedSize"), header->getPropInt("@rowSize"), header->getPropBool("@variableWidth"), header->getPropBool("@quickCompressed"), header->getPropInt("@nodeSize", NODESIZE));
        size32_t rowsize = header->getPropInt("@rowSize");
        bool isvar = header->getPropBool("@variableWidth");
        buffer.init(rowsize,isvar);
    }


    void flush()
    {
        // not needed?
    }

    virtual void putRow(const void *src)
    {
        buffer.deserialize(src);
        writer.put(buffer);
        free((void *)src);
    }


    offset_t getPosition()
    {
        return writer.getPosition();
    }

};



class KeyDiffHeader
{
public:
    KeyDiffHeader() : version(0, 0), minPatchVersion(0, 0), oldCRC(0), newCRC(0), patchCRC(0), tlkInfo(false), tlkCRC(0) {}
    KeyDiffHeader(KeyDiffVersion const & _version, KeyDiffVersion const & _minPatchVersion, char const * _oldIndex, char const * _newIndex, char const * _newTLK)
        : version(_version), minPatchVersion(_minPatchVersion), oldCRC(0), newCRC(0), patchCRC(0), oldIndex(_oldIndex), newIndex(_newIndex), tlkInfo(false), tlkCRC(0)
    {
        if(_newTLK != 0)
        {
            tlkInfo = true;
            newTLK.append(_newTLK);
        }
        crcStreamPos = namesStreamPos = endStreamPos = 0;
    }

    KeyDiffVersion const & queryVersion() const { return version; }
    KeyDiffVersion const & queryMinPatchVersion() const { return minPatchVersion; }
    unsigned queryOldCRC() const { return oldCRC; }
    unsigned queryNewCRC() const { return newCRC; }
    unsigned queryPatchCRC() const { return patchCRC; }
    unsigned queryTLKCRC() const { return tlkCRC; }
    char const * queryOldIndex() const { return oldIndex.str(); }
    char const * queryNewIndex() const { return newIndex.str(); }
    bool hasTLKInfo() const { return tlkInfo; }
    char const * queryNewTLK() const { return tlkInfo ? newTLK.str() : 0; }

    void write(IFileIOStream * _stream)
    {
        stream.set(_stream);
        MemoryBuffer buff;
        buff.append(7, KEYDIFFSIG);
        version.serialize(buff);
        minPatchVersion.serialize(buff);
        crcStreamPos = buff.length();
        crcHeadVer.tally((size32_t)crcStreamPos, buff.toByteArray());
        buff.append(oldCRC).append(newCRC).append(patchCRC).append(tlkCRC);
        namesStreamPos = buff.length();
        crcHeadCRCs.tally((size32_t)(namesStreamPos - crcStreamPos), buff.toByteArray() + crcStreamPos);
        size32_t oil = oldIndex.length();
        size32_t nil = newIndex.length();
        size32_t tlkl = tlkInfo ? newTLK.length() : 0;
        buff.append(oil).append(nil).append(tlkl).append(oil, oldIndex.str()).append(nil, newIndex.str());
        if(tlkl)
            buff.append(tlkl, newTLK.str());
        endStreamPos = buff.length();
        crcHeadNames.tally((size32_t)(endStreamPos - namesStreamPos), buff.toByteArray() + namesStreamPos);
        stream->write(buff.length(), buff.toByteArray());
    }

    void rewriteCRC(unsigned _oldCRC, unsigned _newCRC, unsigned _patchCRC)
    {
        oldCRC = _oldCRC;
        newCRC = _newCRC;
        patchCRC = _patchCRC;
        if(tlkInfo)
            readTLKCRC();
        MemoryBuffer buff;
        buff.append(oldCRC).append(newCRC).append(patchCRC).append(tlkCRC);
        stream->flush();
        stream->seek(crcStreamPos, IFSbegin);
        stream->write(buff.length(), buff.toByteArray());
        crcHeadCRCs.reset();
        assertex(buff.length() == (namesStreamPos - crcStreamPos));
        crcHeadCRCs.tally(buff.length(), buff.toByteArray());
    }

    void readVersionInfo(IFileIOStream * _stream, char const * patchName)
    {
        stream.set(_stream);
        MemoryBuffer buff;
        size32_t bufflen = 7 + 2*KeyDiffVersion::querySerializedSize();
        stream->read(bufflen, buff.reserve(bufflen));
        char signature[7];
        buff.read(7, signature);
        if(strncmp(signature, KEYDIFFSIG, 7) != 0)
            throw MakeStringException(0, "Bad format in file %s, did not appear to be key patch file", patchName);
        version.deserialize(buff);
        minPatchVersion.deserialize(buff);
    }

    void readFileInfo()
    {
        MemoryBuffer buff;
        size32_t bufflen = 4*sizeof(unsigned) + 3*sizeof(size32_t);
        stream->read(bufflen, buff.reserve(bufflen));
        buff.read(oldCRC).read(newCRC).read(patchCRC).read(tlkCRC);
        size32_t oil, nil, tlkl;
        buff.read(oil).read(nil).read(tlkl);
        stream->read(oil, oldIndex.reserve(oil));
        stream->read(nil, newIndex.reserve(nil));
        if(tlkl)
        {
            tlkInfo = true;
            stream->read(tlkl, newTLK.reserve(tlkl));
        }
    }

    unsigned mergeFileCRC(offset_t datasize, unsigned datacrc)
    {
        CRC32Merger merger;
        merger.addChildCRC(crcStreamPos, crcHeadVer.get(), true);
        merger.addChildCRC(namesStreamPos - crcStreamPos, crcHeadCRCs.get(), true);
        merger.addChildCRC(endStreamPos - namesStreamPos, crcHeadNames.get(), true);
        merger.addChildCRC(datasize, datacrc, true);
        return merger.get();
    }

private:
    void readTLKCRC()
    {
        CKeyReader tlkReader(newTLK);
        tlkReader.getRawToEnd();
        tlkCRC = tlkReader.queryCRC();
    }

private:
    Owned<IFileIOStream> stream;
    offset_t crcStreamPos;
    offset_t namesStreamPos;
    offset_t endStreamPos;
    KeyDiffVersion version;
    KeyDiffVersion minPatchVersion;
    unsigned oldCRC;
    unsigned newCRC;
    unsigned patchCRC;
    StringBuffer oldIndex;
    StringBuffer newIndex;
    bool tlkInfo;
    StringBuffer newTLK;
    unsigned tlkCRC;
    CRC32 crcHeadVer, crcHeadCRCs, crcHeadNames;
};

class CKeyDiff : public CInterface
{
public:
    typedef enum {
        CMD_END             = 0,
        CMD_MATCH           = 1, // new curr == old curr
        CMD_FPOS            = 2, // new curr == old curr but fpos has changed, new fpos follows
        CMD_DIFF_OLD_CURR   = 3, // diff between new curr and old curr follows
        CMD_DIFF_OLD_PREV   = 4, // diff between new curr and old prev follows
        CMD_DIFF_NEW_PREV   = 5, // diff between new curr and new prev follows
        CMD_SKIP            = 6, // +N-1, skip N old records
        MAX_SKIP            = 249
    } CommandCode;
    static KeyDiffVersion const version;
    static KeyDiffVersion const minDiffVersionForPatch;
    static KeyDiffVersion const minPatchVersionForDiff;

    CKeyDiff() {}
    CKeyDiff(char const * oldIndex, char const * newIndex, char const * newTLK) 
        : header(version, minPatchVersionForDiff, oldIndex, newIndex, newTLK) 
    {
    }

protected:
    KeyDiffHeader header;
    Owned<IFile> file;
    Owned<IFileIO> fileIO;
    Owned<IFileIOStream> stream;
    static size32_t const streambuffsize;
    static size32_t const compressThreshold;
};

class KeyDiffStats
{
public:
    KeyDiffStats() : stats(new unsigned[CKeyDiff::CMD_SKIP-1]), diffSize(0)
    {
        unsigned i;
        for(i=1; i<CKeyDiff::CMD_SKIP; i++)
            stats[i-1] = 0;
    }
    ~KeyDiffStats() { delete [] stats; }
    void inc(CKeyDiff::CommandCode cmd) { assertex(cmd < CKeyDiff::CMD_SKIP); assertex(cmd > 0); stats[cmd-1]++; }
    void addDiffSize(size32_t sz) { diffSize += sz; }
    void log() const
    {
        LOG(MCstats, "Matching rows: %u", stats[CKeyDiff::CMD_MATCH-1]);
        LOG(MCstats, "Rows close to previous old row: %u", stats[CKeyDiff::CMD_DIFF_OLD_PREV-1]);
        LOG(MCstats, "Rows close to current old row: %u", stats[CKeyDiff::CMD_DIFF_OLD_CURR-1]);
        LOG(MCstats, "Rows close to previous new row: %u", stats[CKeyDiff::CMD_DIFF_NEW_PREV-1]);
        unsigned diffNum = stats[CKeyDiff::CMD_DIFF_OLD_PREV-1] + stats[CKeyDiff::CMD_DIFF_OLD_CURR-1] + stats[CKeyDiff::CMD_DIFF_NEW_PREV-1];
        if(diffNum > 0)
            LOG(MCstats, "Average diff size: %u", ((diffSize + diffNum/2) / diffNum));
    }

private:
    unsigned * stats;
    size32_t diffSize;
};

class CWritableKeyDiff : public CKeyDiff
{
public:
    CWritableKeyDiff(char const * filename, bool overwrite, char const * oldIndex, char const * newIndex, char const * newTLK, unsigned _compmode) 
        : CKeyDiff(oldIndex, newIndex, newTLK)
    {
        file.setown(createIFile(filename));
        if(!overwrite && (file->isFile() != notFound))
            throw MakeStringException(0, "Found preexisting key patch file %s (overwrite not selected)", filename);
        fileIO.setown(file->open(IFOcreate));
        if(!fileIO)
            throw MakeStringException(0, "Could not write key patch file %s", filename);
        stream.setown(createIOStream(fileIO));
        compmode = _compmode;
        datasize = 0;
    }

    void writeHeader()
    {
        header.write(stream);
    }

    void rewriteHeaderCRC(unsigned oldCRC, unsigned newCRC)
    {
        header.rewriteCRC(oldCRC, newCRC, crc.get());
    }

    void writeSkip(unsigned count)
    {
        outb(CMD_SKIP + count - 1);
    }

    void writeMatch()
    {
        stats.inc(CMD_MATCH);
        outb(CMD_MATCH);
    }

    void writeDiff(CommandCode code, char * ptr, size32_t sz)
    {
        stats.inc(code);
        stats.addDiffSize(sz);
        outb(code);
        out(ptr, sz);
    }

    void finish()
    {
        out(NULL, 0);
        outTerminate();
    }

    void logStats() const
    {
        stats.log();
    }

    unsigned queryCRC()
    {
        return crc.get();
    }

    unsigned queryFileCRC()
    {
        return header.mergeFileCRC(datasize, datacrc.get());
    }

private:
    void out(char * ptr, size32_t sz)
    {
        if(!ptr || (outbuff.length()+sz > streambuffsize))
            writeBuff();
        outbuff.append(sz, ptr);
    }

    void outb(byte b)
    {
        if(outbuff.length() >= streambuffsize)
            writeBuff();
        outbuff.append(b);
    }

    void outTerminate()
    {
        size32_t zero = 0;
        stream->write(sizeof(zero), &zero);
        stream->write(sizeof(zero), &zero);
        datacrc.tally(sizeof(zero), &zero);
        datacrc.tally(sizeof(zero), &zero);
        datasize += 2*sizeof(zero);
    }

    void writeBuff()
    {
        size32_t outsize = outbuff.length();
        size32_t wrsize = outsize;
        void const * wrbuff = outbuff.toByteArray();
        crc.tally(wrsize, wrbuff);
        MemoryAttr ma;
        MemoryBuffer mb;
        size32_t wrflag = wrsize;
        if(compmode && (outsize > compressThreshold))
        {
            size32_t newsize = outsize*4/5; // only compress if get better than 80%
            if (compmode==COMPRESS_METHOD_LZW) {
                byte *compbuff = (byte *)ma.allocate(streambuffsize);
                Owned<ICompressor> compressor = createLZWCompressor();
                compressor->open(compbuff, newsize);
                if (compressor->write(wrbuff, outsize)==outsize) {
                    compressor->close();
                    wrsize = compressor->buflen();
                    wrflag = wrsize;
                    wrbuff = compbuff;
                }
            }
            else if (compmode==COMPRESS_METHOD_LZMA) {
                LZMACompressToBuffer(mb,outsize,wrbuff);
                if (mb.length()+16<outsize) {
                    wrsize = mb.length();
                    wrflag = wrsize|LZMA_FLAG;
                    wrbuff = mb.bufferBase();
                }
            }
            else
                throw MakeStringException(-1,"Unknown compression mode (%d)",compmode);
        }
        stream->write(sizeof(outsize), &outsize);
        stream->write(sizeof(wrflag), &wrflag);
        stream->write(wrsize, wrbuff);
        datacrc.tally(sizeof(outsize), &outsize);
        datacrc.tally(sizeof(wrsize), &wrsize);
        datacrc.tally(wrsize, wrbuff);
        datasize += (sizeof(outsize) + sizeof(wrflag) + wrsize);
        outbuff.clear();
    }

private:
    unsigned compmode;  // 0, COMPRESS_METHOD_LZW or COMPRESS_METHOD_LZMA
    MemoryBuffer outbuff;
    CRC32 crc;
    CRC32 datacrc;
    offset_t datasize;
    KeyDiffStats stats;
};

class CReadableKeyDiff : public CKeyDiff
{
public:
    CReadableKeyDiff(char const * filename) 
        : patch(filename), eof(false), insize(0), lastSkipCount(0)
    {
        file.setown(createIFile(filename));
        fileIO.setown(file->open(IFOread));
        if(!fileIO)
            throw MakeStringException(0, "Could not read key patch file %s", filename);
        stream.setown(createIOStream(fileIO));
        inbuff.reserve(streambuffsize + 2*sizeof(size32_t));
        inbuff.rewrite(0);
    }

    KeyDiffHeader const & queryHeader() const { return header; }

    bool compatibleVersions(StringBuffer & error) const
    {
        if(header.queryVersion() < minDiffVersionForPatch)
        {
            error.appendf("Patch was created with keydiff version %u.%u, this keypatch requires at least keydiff version %u.%u", header.queryVersion().queryMajor(), header.queryVersion().queryMinor(), minDiffVersionForPatch.queryMajor(), minDiffVersionForPatch.queryMinor());
            return false;
        }
        if(version < header.queryMinPatchVersion())
        {
            error.appendf("This is keypatch version %u.%u, this patch requires at least keypatch version %u.%u", version.queryMajor(), version.queryMinor(), header.queryMinPatchVersion().queryMajor(), header.queryMinPatchVersion().queryMinor());
            return false;
        }
        return true;
    }

    void readHeaderVersionInfo()
    {
        header.readVersionInfo(stream, patch.get());
    }

    void readHeaderFileInfo()
    {
        header.readFileInfo();
        size32_t insize, rdsize;
        stream->read(sizeof(insize), &insize);
        stream->read(sizeof(rdsize), &rdsize);
        inbuff.append(insize);
        inbuff.append(rdsize);
        inbuff.reset(0);
    }

    CommandCode readCmd()
    {
        byte cmd;
        if(!inb(cmd))
            return CMD_END;
        if(cmd >= CMD_SKIP)
        {
            lastSkipCount = cmd - CMD_SKIP + 1;
            return CMD_SKIP;
        }
        return static_cast<CommandCode>(cmd);
    }

    void readNewFPos(RowBuffer & buffer)
    {
        offset_t fpos;
        infpos(fpos);
        buffer.setFPos(fpos);
    }

    unsigned readSkipCount()
    {
        return lastSkipCount;
    }

    bool readDiffAndExpand(RowBuffer const & prev, RowBuffer & dest)
    {
        byte const * src = in();
        if(!src) return false;
        size32_t consumed = dest.diffExpand(src, prev);
        return skip(consumed);
    }

    unsigned queryCRC()
    {
        return crc.get();
    }

private:
    byte const * in()
    {
        if(insize == 0)
            if(!readBuff())
                return NULL;
        return inbuff.readDirect(0);
    }

    bool skip(size32_t sz)
    {
        if(insize < sz)
            return false;
        insize -= sz;
        inbuff.skip(sz);
        return true;
    }

    bool inb(byte & b)
    {
        if(insize == 0)
            if(!readBuff())
                return false;
        if(insize == 0)
            return false;
        inbuff.read(b);
        insize--;
        return true;
    }

    bool infpos(offset_t & fp)
    {
        if(insize == 0)
            if(!readBuff())
                return false;
        if(insize < sizeof(offset_t))
            return false;
        inbuff.read(fp);
        insize -= sizeof(offset_t);
        return true;
    }

    bool readBuff()
    {
        if(eof)
            return false;
        size32_t rdsize;
        inbuff.read(insize);
        inbuff.read(rdsize);
        if(insize == 0)
        {
            eof = true;
            inbuff.clear();
            insize = 0;
            return false;
        }
        inbuff.rewrite(0);
        if(insize == rdsize)
        {
            stream->read(rdsize + 2*sizeof(size32_t), inbuff.reserve(rdsize + 2*sizeof(size32_t)));
        }
        else
        {
            bool fastlz = false;
            if (rdsize&LZMA_FLAG) {
                fastlz = true;
                rdsize &= ~LZMA_FLAG;
            }
            byte * buf;
            if (compma.length()<rdsize)
                buf = (byte *)compma.allocate(rdsize+4096);
            else
                buf = (byte *)compma.bufferBase();
            stream->read(rdsize, buf);
            if (fastlz) 
                LZMADecompressToBuffer(inbuff,buf);
            else {
                Owned<IExpander> expander = createLZWExpander();
                size32_t expsize = expander->init(buf);
                if(expsize != insize) 
                    throw MakeStringException(0, "LZW compression/expansion error");
                expander->expand(inbuff.reserve(insize));
            }
            stream->read(2*sizeof(size32_t), inbuff.reserve(2*sizeof(size32_t)));
        }
        crc.tally(insize, inbuff.toByteArray());
        return true;
    }

private:
    StringAttr patch;
    CRC32 crc;
    bool eof;
    MemoryBuffer inbuff;
    size32_t insize;
    unsigned lastSkipCount;
    MemoryAttr compma;
};

class CKeyDiffGenerator : public CInterface, public IKeyDiffGenerator
{
public:
    IMPLEMENT_IINTERFACE;

    CKeyDiffGenerator(char const * oldIndex, char const * newIndex, char const * patch, char const * newTLK, bool overwrite, unsigned compmode)
        : oldInput(oldIndex), newInput(newIndex), keydiff(patch, overwrite, oldIndex, newIndex, newTLK, compmode), keyedsize(oldInput.queryKeyedSize()), rowsize(oldInput.queryRowSize())
    {
        if((newInput.queryKeyedSize() != keyedsize) || (newInput.queryRowSize() != rowsize))
            throw MakeStringException(0, "Cannot generate diff for keys with different record sizes");
        if(newInput.isVariableWidth() != oldInput.isVariableWidth())
            throw MakeStringException(0, "Old and new keys are of different types (%s is variable width)", (oldInput.isVariableWidth() ? "old" : "new"));
        if(newInput.isQuickCompressed() != oldInput.isQuickCompressed())
            throw MakeStringException(0, "Old and new keys are of different types (%s is quick compressed)", (oldInput.isQuickCompressed() ? "old" : "new"));
        newcurr.init(rowsize, oldInput.isVariableWidth());
        newprev.init(rowsize, oldInput.isVariableWidth());
        oldcurr.init(rowsize, oldInput.isVariableWidth());
        oldprev.init(rowsize, oldInput.isVariableWidth());
        size32_t diffsize = (rowsize + sizeof(offset_t)) * 2; // *2 is excessive
        if(oldInput.isVariableWidth()) diffsize += sizeof(size32_t); // as have to store size
        diffnewprev = (char *)malloc(diffsize);
        diffoldcurr = (char *)malloc(diffsize);
        diffoldprev = (char *)malloc(diffsize);
    }
                                                                                                                   
    ~CKeyDiffGenerator()
    {
        free(diffnewprev);
        free(diffoldcurr);
        free(diffoldprev);
    }

    virtual void run()
    {
        keydiff.writeHeader();
        writeBody();
        keydiff.finish();
        oldInput.getToEnd(oldcurr);
        keydiff.rewriteHeaderCRC(oldInput.queryCRC(), newInput.queryCRC());
    }

    virtual void logStats() const
    {
        LOG(MCstats, "Rows in old index: %u", oldInput.queryCount());
        LOG(MCstats, "Rows in new index: %u", newInput.queryCount());
        keydiff.logStats();
    }

    virtual unsigned queryPatchCRC()
    {
        return keydiff.queryCRC();
    }

    virtual unsigned queryPatchFileCRC()
    {
        return keydiff.queryFileCRC();
    }

    virtual void setProgressCallback(IKeyDiffProgressCallback * callback, offset_t freq)
    {
        oldInput.setProgressCallback(callback, freq);
    }

private:
    bool readNew()
    {
        newcurr.swap(newprev);
        return newInput.get(newcurr);
    }

    bool readOld()
    {
        oldcurr.swap(oldprev);
        if(oldInput.get(oldcurr))
            return true;
        oldcurr.clear();
        return false;
    }

    void writeBody()
    {
        if(!readNew())
            return;
        bool eosold = !readOld();
        while(true)
        {
            int cmp = -1;
            unsigned skipcount = 0;
            size32_t doc = (size32_t)-1;
            while (!eosold&&(skipcount < CKeyDiff::MAX_SKIP))
            {
                cmp = newcurr.compareKeyed(oldcurr);
                if(cmp <= 0)
                    break;
#ifdef SMALL_ENOUGH_RATIO
                size32_t ndoc = newcurr.diffCompress(oldcurr, diffoldcurr);
                if (ndoc<newcurr.rowSize()/SMALL_ENOUGH_RATIO) {
                    doc = ndoc;
                    cmp = -1;
                    break;
                }
#endif
                skipcount++;
                eosold = !readOld();
            }
            if(skipcount)
                keydiff.writeSkip(skipcount);
            if (eosold||((cmp==0) && (newcurr.queryFPos() != oldcurr.queryFPos())))
                cmp = -1;
            if(cmp==0)
            {
                keydiff.writeMatch();
                if(!readNew())
                    break;
            }
            else if(cmp<0)
            {
                if (doc!=(size32_t)-1)
                    keydiff.writeDiff(CKeyDiff::CMD_DIFF_OLD_CURR, diffoldcurr, doc);
                else {
                    size32_t dnp = newcurr.diffCompress(newprev, diffnewprev);
                    size32_t dop = newcurr.diffCompress(oldprev, diffoldprev);
                    doc = newcurr.diffCompress(oldcurr, diffoldcurr);
                    if(dnp<dop)
                    {
                        if(dnp<doc)
                            keydiff.writeDiff(CKeyDiff::CMD_DIFF_NEW_PREV, diffnewprev, dnp);
                        else
                            keydiff.writeDiff(CKeyDiff::CMD_DIFF_OLD_CURR, diffoldcurr, doc);
                    }
                    else if(dop<doc)
                        keydiff.writeDiff(CKeyDiff::CMD_DIFF_OLD_PREV, diffoldprev, dop);
                    else
                        keydiff.writeDiff(CKeyDiff::CMD_DIFF_OLD_CURR, diffoldcurr, doc);
                }
                if(!readNew())
                    break;
            }
        }
    }

private:
    CKeyReader oldInput;
    CKeyReader newInput;
    CWritableKeyDiff keydiff;
    size32_t keyedsize;
    size32_t rowsize;
    RowBuffer newcurr;
    RowBuffer newprev;
    RowBuffer oldcurr;
    RowBuffer oldprev;
    char * diffnewprev;
    char * diffoldcurr;
    char * diffoldprev;
};

class CTLKGenerator : public Thread
{
public:
    CTLKGenerator(INodeReceiver * receiver, unsigned numParts, KeyDiffHeader const & _header) : tlkReceiver(receiver), remaining(numParts), header(_header)
    {
    }

    void open(char const * tlkName, bool overwrite, unsigned keyedsize, unsigned rowsize, bool variableWidth, bool quickCompressed, unsigned nodeSize)
    {
        filename.set(tlkName);
        writer.setown(new CKeyWriter());
        writer->init(tlkName, overwrite, keyedsize, rowsize, variableWidth, quickCompressed, nodeSize);
    }

    virtual int run()
    {
        Owned<CNodeInfo> ni;
        while(remaining)
        {
            ni.setown(new CNodeInfo);
            if(tlkReceiver->recv(*ni))
                addNode(LINK(ni));
        }
        return 0;
    }

    bool addNode(CNodeInfo * info)
    {
        CriticalBlock block(crit);
        TLKnodes.append(*info);
        remaining--;
        if(remaining==0)
            finish();
        return (remaining>0);
    }

private:
    void finish()
    {
        PROGLOG("Received all TLK data, generating TLK");
        tlkReceiver->stop();
        TLKnodes.sort(rowCompare);
        if(TLKnodes.length())
        {
            CNodeInfo & lastNode = TLKnodes.item(TLKnodes.length()-1);
            memset(lastNode.value, 0xff, lastNode.size);
        }
        offset_t fp = 1;
        ForEachItemIn(idx, TLKnodes) {
            CNodeInfo & info = TLKnodes.item(idx);
            info.pos = fp++;
            writer->putNode(info);
        }
        if(header.hasTLKInfo())
        {
            if(writer->queryCRC() != header.queryTLKCRC())
                WARNLOG("CRC mismatch: on keydiff, new TLK %s had key CRC %08X, while on keypatch, new TLK %s had key CRC %08X", header.queryNewTLK(), header.queryTLKCRC(), filename.get(), writer->queryCRC());
        }
        else
            WARNLOG("Patch did not include TLK info in header, TLK has been generated but its CRC has not been verified");
    }

    static int rowCompare(IInterface * const * ll, IInterface * const * rr)
    {
        CNodeInfo * l = static_cast<CNodeInfo *>(*ll);
        CNodeInfo * r = static_cast<CNodeInfo *>(*rr);
        return memcmp(l->value, r->value, l->size);
    }

private:
    Owned<INodeReceiver> tlkReceiver;
    unsigned remaining;
    KeyDiffHeader const & header;
    StringAttr filename;
    Owned<CKeyWriter> writer;
    CriticalSection crit;
    NodeInfoArray TLKnodes;
};

class CKeyDiffApplicator : public CInterface, public IKeyDiffApplicator
{
public:
    IMPLEMENT_IINTERFACE;

    CKeyDiffApplicator(char const * patch, char const * _oldIndex, char const * _newIndex, char const * _newTLK, bool _overwrite, bool _ignoreTLK)
        : oldIndex(_oldIndex), newIndex(_newIndex), newTLK(_newTLK), overwrite(_overwrite), ignoreTLK(_ignoreTLK), keydiff(patch), keyedsize(0), rowsize(0)
    {
    }

    ~CKeyDiffApplicator()
    {
    }

    virtual void setTransmitTLK(INodeSender * sender)
    {
        tlkSender.setown(sender);
    }

    virtual void setReceiveTLK(INodeReceiver * receiver, unsigned numParts)
    {
        tlkGen.setown(new CTLKGenerator(receiver, numParts, keydiff.queryHeader()));
    }

    virtual void run()
    {
        init();
        readOld(1);
        bool more = true;
        while(more)
        {
            CKeyDiff::CommandCode cmd = keydiff.readCmd();
            switch(cmd)
            {
            case CKeyDiff::CMD_END:
                more = false;
                break;
            case CKeyDiff::CMD_MATCH:
                newcurr.copyFrom(oldcurr);
                writeNew();
                break;
            case CKeyDiff::CMD_SKIP:
                readOld(keydiff.readSkipCount());
                break;
            case CKeyDiff::CMD_DIFF_OLD_CURR:
                doDiff(oldcurr);
                break;
            case CKeyDiff::CMD_DIFF_OLD_PREV:
                doDiff(oldprev);
                break;
            case CKeyDiff::CMD_DIFF_NEW_PREV:
                doDiff(newprev);
                break;
            case CKeyDiff::CMD_FPOS:
            default:
                UNIMPLEMENTED;
            }
        }
        verifyCRCs();
        if(newOutput->queryCount())
        {
            if(tlkGen)
            {
                bool wait = tlkGen->addNode(newprev.getNodeInfo(newOutput->queryCount()-1));
                if(wait) PROGLOG("Waiting for remaining TLK data");
                tlkGen->join();
            }
            else if(tlkSender)
            {
                Owned<CNodeInfo> ni(newprev.getNodeInfo(newOutput->queryCount()-1));
                tlkSender->send(*ni);
            }
        }
    }

    virtual void getHeaderVersionInfo(unsigned short & versionMajor, unsigned short & versionMinor, unsigned short & minPatchVersionMajor, unsigned short & minPatchVersionMinor)
    {
        keydiff.readHeaderVersionInfo();
        versionMajor = keydiff.queryHeader().queryVersion().queryMajor();
        versionMinor = keydiff.queryHeader().queryVersion().queryMinor();
        minPatchVersionMajor = keydiff.queryHeader().queryMinPatchVersion().queryMajor();
        minPatchVersionMinor = keydiff.queryHeader().queryMinPatchVersion().queryMinor();
    }

    virtual void getHeaderFileInfo(StringAttr & oldindex, StringAttr & newindex, bool & tlkInfo, StringAttr & newTLK)
    {
        keydiff.readHeaderFileInfo();
        oldindex.set(keydiff.queryHeader().queryOldIndex());
        newindex.set(keydiff.queryHeader().queryNewIndex());
        tlkInfo = keydiff.queryHeader().hasTLKInfo();
        if(tlkInfo)
            newTLK.set(keydiff.queryHeader().queryNewTLK());
    }

    virtual bool compatibleVersions(StringBuffer & error) const
    {
        return keydiff.compatibleVersions(error);
    }

    virtual void setProgressCallback(IKeyDiffProgressCallback * callback, offset_t freq)
    {
        progressCallback.setown(callback);
        progressFrequency = freq;
    }

private:
    void init()
    {
        keydiff.readHeaderVersionInfo();
        StringBuffer versionError;
        if(!keydiff.compatibleVersions(versionError))
            throw MakeStringExceptionDirect(0, versionError.str());
        keydiff.readHeaderFileInfo();
        if(!oldIndex.get())
            oldIndex.set(keydiff.queryHeader().queryOldIndex());
        if(!newIndex.get())
            newIndex.set(keydiff.queryHeader().queryNewIndex());
        if(tlkGen)
        {
            if(!newTLK.get())
            {
                if(keydiff.queryHeader().hasTLKInfo())
                    newTLK.set(keydiff.queryHeader().queryNewTLK());
                else
                    throw MakeStringException(0, "Trying to generate TLK using filename from patch, but patch does not include TLK header information");
            }
        }
        else if(keydiff.queryHeader().hasTLKInfo() && !ignoreTLK)
            throw MakeStringException(0, "Patch includes TLK header information, but TLK generation not enabled --- aborting, invoke with warning suppressed to go ahead");
        oldInput.setown(new CKeyReader(oldIndex));
        keyedsize = oldInput->queryKeyedSize();
        if(progressCallback)
            oldInput->setProgressCallback(progressCallback.getLink(), progressFrequency);
        rowsize = oldInput->queryRowSize();
        newOutput.setown(new CKeyWriter());
        newOutput->init(newIndex, overwrite, keyedsize, rowsize, oldInput->isVariableWidth(), oldInput->isQuickCompressed(), oldInput->getNodeSize());
        if(tlkGen)
            tlkGen->open(newTLK, overwrite, keyedsize, rowsize, oldInput->isVariableWidth(), oldInput->isQuickCompressed(), oldInput->getNodeSize());
        newcurr.init(rowsize, oldInput->isVariableWidth());
        newprev.init(rowsize, oldInput->isVariableWidth());
        oldcurr.init(rowsize, oldInput->isVariableWidth());
        oldprev.init(rowsize, oldInput->isVariableWidth());
        if(tlkGen)
            tlkGen->start();
    }

    bool readOld(unsigned count)
    {
        while(count--)
        {
            oldcurr.swap(oldprev);
            if(!oldInput->get(oldcurr)) 
            {
                oldcurr.clear();
                return false;
            }
        }
        return true;
    }

    void doDiff(RowBuffer const & prev)
    {
        bool ok = keydiff.readDiffAndExpand(prev, newcurr);
        if(!ok)
            throw MakeStringException(0, "Error in patch file");
        writeNew();
    }

    void writeNew()
    {
        newOutput->put(newcurr);
        newcurr.swap(newprev);
    }

    void verifyCRCs()
    {
        oldInput->getToEnd(oldcurr);
        if(oldInput->queryCRC() != keydiff.queryHeader().queryOldCRC())
            WARNLOG("CRC mismatch: on keydiff, old index %s had key CRC %08X, while on keypatch, old index %s had key CRC %08X", keydiff.queryHeader().queryOldIndex(), keydiff.queryHeader().queryOldCRC(), oldIndex.get(), oldInput->queryCRC());
        if(newOutput->queryCRC() != keydiff.queryHeader().queryNewCRC())
            WARNLOG("CRC mismatch: on keydiff, new index %s had key CRC %08X, while on keypatch, new index %s generated with key CRC %08X", keydiff.queryHeader().queryNewIndex(), keydiff.queryHeader().queryNewCRC(), newIndex.get(), newOutput->queryCRC());
        if(keydiff.queryCRC() != keydiff.queryHeader().queryPatchCRC())
            WARNLOG("CRC mismatch: on keydiff, the patch was generated with block CRC %08X, while on keypatch, it was read with block CRC %08X: looks like there has been a file corruption", keydiff.queryHeader().queryPatchCRC(), keydiff.queryCRC());
    }

private:
    StringAttr oldIndex;
    StringAttr newIndex;
    StringAttr newTLK;
    bool overwrite;
    bool ignoreTLK;
    CReadableKeyDiff keydiff;
    Owned<CKeyReader> oldInput;
    Owned<CKeyWriter> newOutput;
    size32_t keyedsize;
    size32_t rowsize;
    RowBuffer newcurr;
    RowBuffer newprev;
    RowBuffer oldcurr;
    RowBuffer oldprev;
    Owned<CTLKGenerator> tlkGen;
    Owned<INodeSender> tlkSender;
    Owned<IKeyDiffProgressCallback> progressCallback;
    offset_t progressFrequency;
};

IKeyDiffGenerator * createKeyDiffGenerator(char const * oldIndex, char const * newIndex, char const * patch, char const * newTLK, bool overwrite, unsigned compmode)
{
    return new CKeyDiffGenerator(oldIndex, newIndex, patch, newTLK, overwrite, compmode);
}

IKeyDiffApplicator * createKeyDiffApplicator(char const * patch, bool overwrite, bool ignoreTLK)
{
    return new CKeyDiffApplicator(patch, 0, 0, 0, overwrite, ignoreTLK);
}

IKeyDiffApplicator * createKeyDiffApplicator(char const * patch, char const * oldIndex, char const * newIndex, char const * newTLK, bool overwrite, bool ignoreTLK)
{
    return new CKeyDiffApplicator(patch, oldIndex, newIndex, newTLK, overwrite, ignoreTLK);
}

StringBuffer & getKeyDiffVersion(StringBuffer & buff)
{
    return buff.append(CKeyDiff::version.queryMajor()).append('.').append(CKeyDiff::version.queryMinor());
}

StringBuffer & getKeyDiffMinDiffVersionForPatch(StringBuffer & buff)
{
    return buff.append(CKeyDiff::minDiffVersionForPatch.queryMajor()).append('.').append(CKeyDiff::minDiffVersionForPatch.queryMinor());
}

StringBuffer & getKeyDiffMinPatchVersionForDiff(StringBuffer & buff)
{
    return buff.append(CKeyDiff::minPatchVersionForDiff.queryMajor()).append('.').append(CKeyDiff::minPatchVersionForDiff.queryMinor());
}

IKeyFileRowReader *createKeyFileReader(const char *filename)
{
    return new CKeyFileReader(filename);
}

IKeyFileRowWriter *createKeyWriter(const char *filename,IPropertyTree *header, bool overwrite, unsigned nodeSize)
{
    return new CKeyFileWriter(filename,header,overwrite, nodeSize);
}


/* To apply a patch, we require that
     (a) the version of keydiff which generated the patch should be at least the minDiffVersionForPatch of the keypatch which applies it,
 AND (b) the version of keypatch which applies the patch should be at least the minPatchVersionForDiff of the keydiff which generated it. */

KeyDiffVersion const CKeyDiff::version(1, 0);
KeyDiffVersion const CKeyDiff::minDiffVersionForPatch(0, 8);
KeyDiffVersion const CKeyDiff::minPatchVersionForDiff(1, 0);        // version 1 for fastLZ
size32_t const CKeyDiff::streambuffsize = 0x20000;
size32_t const CKeyDiff::compressThreshold = 0x1000;



