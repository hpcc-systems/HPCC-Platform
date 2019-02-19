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
#include "jptree.hpp"

#include "fterror.hpp"
#include "dadfs.hpp"
#include "ftbase.ipp"
#include "daftmc.hpp"
#include "dasds.hpp"
#include "daftcfg.hpp"
#include "environment.hpp"
#include "dalienv.hpp"
#include "rmtspawn.hpp"


//----------------------------------------------------------------------------

void getDfuTempName(RemoteFilename & temp, const RemoteFilename & src)
{
    StringBuffer ext;
    src.split(NULL, NULL, NULL, &ext);
    ext.append(".tmp");

    temp.set(src);
    temp.setExtension(ext);
}

void renameDfuTempToFinal(const RemoteFilename & realname)
{
    RemoteFilename tempFilename;
    StringBuffer newTailname;
    getDfuTempName(tempFilename, realname);
    realname.getTail(newTailname);

    OwnedIFile output = createIFile(tempFilename);
    try
    {
        output->rename(newTailname);
    }
    catch (IException * e)
    {
        EXCLOG(e, "Failed to rename target file");
        StringBuffer oldName;
        realname.getPath(oldName);
        LOG(MCdebugInfoDetail, unknownJob, "Error: Rename %s->%s failed - tring to delete target and rename again", oldName.str(), newTailname.str());
        e->Release();
        OwnedIFile old = createIFile(realname);
        old->remove();
        output->rename(newTailname);
    }
}

//----------------------------------------------------------------------------

PartitionPoint::PartitionPoint()
{
    clear();
}

PartitionPoint::PartitionPoint(unsigned _whichInput, unsigned _whichOutput, offset_t _inputOffset, offset_t _inputLength, offset_t _outputLength)
{
    clear();
    whichInput = _whichInput;
    whichOutput = _whichOutput;
    inputOffset = _inputOffset;
    inputLength = _inputLength;
    outputLength = _outputLength;
#ifdef DEBUG
    display();
#endif
}


void PartitionPoint::clear()
{
    whichInput = 0;
    whichOutput = 0;
    inputOffset = 0;
    inputLength = 0;
    outputLength = 0;
    outputOffset = 0;
    whichSlave = (unsigned)-1;
}


void PartitionPoint::deserialize(MemoryBuffer & in)
{
    inputName.deserialize(in);
    outputName.deserialize(in);

    in.read(whichInput);
    in.read(inputOffset);
    in.read(inputLength);
    in.read(whichOutput);
    in.read(outputOffset);
    in.read(outputLength);
    in.read(whichSlave);
    modifiedTime.deserialize(in);
    ::deserialize(in, fixedText);
}

void PartitionPoint::display()
{
    StringBuffer fulli, fullo;
    LOG(MCdebugInfoDetail, unknownJob,
             "Partition %s{%d}[%" I64F "d size %" I64F "d]->%s{%d}[%" I64F "d size %" I64F "d]",
             inputName.getPath(fulli).str(), whichInput, inputOffset, inputLength,
             outputName.getPath(fullo).str(), whichOutput, outputOffset, outputLength);
}


void PartitionPoint::restore(IPropertyTree * tree)
{
    StringBuffer fullname;

    whichInput = tree->getPropInt(ANinput);
    inputOffset = tree->getPropInt64(ANinputOffset);
    inputLength = tree->getPropInt64(ANinputLength);
    whichOutput = tree->getPropInt(ANoutput);
    outputOffset = tree->getPropInt64(ANoutputOffset);
    outputLength = tree->getPropInt64(ANoutputLength);
    setCanAccessDirectly(inputName,tree->getPropInt(ANinputDirect) != 0);
    setCanAccessDirectly(outputName,tree->getPropInt(ANoutputDirect) != 0);
}

void PartitionPoint::serialize(MemoryBuffer & out)
{
    inputName.serialize(out);
    outputName.serialize(out);

    out.append(whichInput);
    out.append(inputOffset);
    out.append(inputLength);
    out.append(whichOutput);
    out.append(outputOffset);
    out.append(outputLength);
    out.append(whichSlave);
    modifiedTime.serialize(out);
    ::serialize(out, fixedText);
}

void PartitionPoint::save(IPropertyTree * tree)
{
    tree->setPropInt(ANinput, whichInput);
    tree->setPropInt64(ANinputOffset, inputOffset);
    tree->setPropInt64(ANinputLength, inputLength);
    tree->setPropInt(ANoutput, whichOutput);
    tree->setPropInt64(ANoutputOffset, outputOffset);
    tree->setPropInt64(ANoutputLength, outputLength);
    tree->setPropInt(ANinputDirect, canAccessDirectly(inputName));
    tree->setPropInt(ANoutputDirect, canAccessDirectly(outputName));
}


//---------------------------------------------------------------------------

const char * FFTtext[FFTlast] = {
    "unknown",
    "fixed", "variable", "blocked",
    "csv",
    "utf",
    "utf-8", "utf-8n",
    "utf-16", "utf-16be", "utf-16le",
    "utf-32", "utf-32be", "utf-32le",
    "recfm-vb", "recfm-v", "variablebigendian"
};

void FileFormat::deserialize(MemoryBuffer & in)
{
    byte tempType;

    in.read(tempType); type = (FileFormatType)tempType;
    switch (type)
    {
    case FFTfixed:
    case FFTblocked:
        in.read(recordSize);
        break;
    case FFTcsv:
    case FFTutf:
    case FFTutf8: case FFTutf8n:
    case FFTutf16: case FFTutf16be: case FFTutf16le:
    case FFTutf32: case FFTutf32be: case FFTutf32le:
        in.read(maxRecordSize);
        ::deserialize(in, separate);
        ::deserialize(in, quote);
        ::deserialize(in, terminate);
        ::deserialize(in, rowTag);
        updateMarkupType(rowTag, NULL); //neither kind nor markup currently serialized.  may add later
        break;
    }
}


void FileFormat::deserializeExtra(MemoryBuffer & in, unsigned version)
{
    switch (type)
    {
    case FFTcsv:
    case FFTutf:
    case FFTutf8: case FFTutf8n:
    case FFTutf16: case FFTutf16be: case FFTutf16le:
    case FFTutf32: case FFTutf32be: case FFTutf32le:
        if (version == 1)
            ::deserialize(in, escape);
        break;
    }
}


unsigned FileFormat::getUnitSize() const
{
    switch (type)
    {
    case FFTfixed:
        return recordSize;
    case FFTblocked:
        return EFX_BLOCK_SIZE;
    case FFTvariable:
    case FFTvariablebigendian:
    case FFTcsv:
    case FFTutf:
    case FFTutf8: case FFTutf8n:
        return 1;
    case FFTutf16: case FFTutf16be: case FFTutf16le:
        return 2;
    case FFTutf32: case FFTutf32be: case FFTutf32le:
        return 4;
    }
    return 1;
}

bool rowLocationIsPath(const char *rowLocator)
{
    if (rowLocator && *rowLocator == '/')
        return true;
    return false;
}

void FileFormat::updateMarkupType(const char *rowLocator, const char *kind)
{
    if (kind)
    {
        if (strieq(kind, "xml"))
            markup = FMTxml;
        else if (strieq(kind, "json"))
            markup = FMTjson;
        else
            markup = FMTunknown;
    }
    else if (rowLocator)
    {
        if (rowLocationIsPath(rowLocator))
            markup = FMTjson;
        else
            markup = FMTxml;
    }
    else
        markup = FMTunknown;
}

bool FileFormat::restore(IPropertyTree * props)
{
    StringBuffer formatText;
    props->getProp(FPformat, formatText);
    const char * format = formatText.str();

    if (stricmp(format, "blocked")==0)
        type = FFTblocked;
    else if (stricmp(format, "variable")==0)
        type = FFTvariable;
    else if (stricmp(format, "variablebigendian")==0)
        type = FFTvariablebigendian;
    else if (stricmp(format, "csv")==0)
    {
        type = FFTcsv;
        maxRecordSize = props->getPropInt(FPmaxRecordSize, DEFAULT_MAX_CSV_SIZE);
        separate.set(props->queryProp(FPcsvSeparate));
        quote.set(props->queryProp(FPcsvQuote));
        terminate.set(props->queryProp(FPcsvTerminate));
        if (props->hasProp(FPcsvEscape))
            escape.set(props->queryProp(FPcsvEscape));
        if (maxRecordSize == 0)
            throwError(DFTERR_MaxRecordSizeZero);
    }
    else if (memicmp(format, "utf", 3) == 0)
    {
        type = FFTutf;
        const char * tail = format + 3;
        if (*tail == '-')
            tail++;
        if (stricmp(tail, "8")==0)
            type = FFTutf8;
        else if (stricmp(tail, "8N")==0)
            type = FFTutf8n;
        else if (stricmp(tail, "16")==0)
            type = FFTutf16;
        else if (stricmp(tail, "16BE")==0)
            type = FFTutf16be;
        else if (stricmp(tail, "16LE")==0)
            type = FFTutf16le;
        else if (stricmp(tail, "32")==0)
            type = FFTutf32;
        else if (stricmp(tail, "32BE")==0)
            type = FFTutf32be;
        else if (stricmp(tail, "32LE")==0)
            type = FFTutf32le;
        else if (*tail)
            throwError1(DFTERR_UnknownUTFFormat, format);
        maxRecordSize = props->getPropInt(FPmaxRecordSize, DEFAULT_MAX_CSV_SIZE);
        separate.set(props->queryProp(FPcsvSeparate));
        quote.set(props->queryProp(FPcsvQuote));
        terminate.set(props->queryProp(FPcsvTerminate));
        if (props->hasProp(FPcsvEscape))
            escape.set(props->queryProp(FPcsvEscape));
        rowTag.set(props->queryProp(FProwTag));
        if (maxRecordSize == 0)
            throwError(DFTERR_MaxRecordSizeZero);
        updateMarkupType(rowTag, props->queryProp(FPkind));
        headerLength = (unsigned)props->getPropInt(FPheaderLength, -1);
        footerLength = (unsigned)props->getPropInt(FPfooterLength, -1);
    }
    else if ((stricmp(format, "recfmvb")==0)||(stricmp(format, "recfm-vb")==0))
        type = FFTrecfmvb;
    else if ((stricmp(format, "recfmv")==0)||(stricmp(format, "recfm-v")==0))
        type = FFTrecfmv;
    else if (props->hasProp(FPrecordSize))
    {
        type = FFTfixed;
        recordSize = props->getPropInt(FPrecordSize);
    }
    else
        return false;
    return true;
}

void FileFormat::save(IPropertyTree * props)
{
    switch (type)
    {
    case FFTfixed:
        props->setPropInt(FPrecordSize, recordSize);
        props->setProp(FPkind, FFTtext[type]);
        break;
    case FFTblocked:
        props->setProp(FPformat, "blocked");
        props->setProp(FPkind, FFTtext[type]);
        break;
    case FFTvariable:
        props->setProp(FPformat, "variable");
        props->setProp(FPkind, FFTtext[type]);
        break;
    case FFTvariablebigendian:
        props->setProp(FPformat, "variablebigendian");
        props->setProp(FPkind, FFTtext[FFTvariable]);
        break;
    case FFTcsv:
    case FFTutf:
    case FFTutf8: case FFTutf8n:
    case FFTutf16: case FFTutf16be: case FFTutf16le:
    case FFTutf32: case FFTutf32be: case FFTutf32le:
        props->setProp(FPformat, FFTtext[type]);
        if (maxRecordSize)  props->setPropInt(FPmaxRecordSize, maxRecordSize);
        if (separate)       props->setProp(FPcsvSeparate, separate);
        if (quote)          props->setProp(FPcsvQuote, quote);
        if (terminate)      props->setProp(FPcsvTerminate, terminate);
        if (escape)         props->setProp(FPcsvEscape, escape);
        if (rowTag)         props->setProp(FProwTag, rowTag);
        if (markup != FMTunknown)
            props->setProp(FPkind, (markup==FMTjson) ? "json" : "xml");
        else
            props->setProp(FPkind, FFTtext[FFTcsv]);
        if (headerLength!=(unsigned)-1)
            props->setPropInt(FPheaderLength, headerLength);
        if (footerLength!=(unsigned)-1)
            props->setPropInt(FPfooterLength, footerLength);
        break;
    case FFTrecfmvb:
    case FFTrecfmv:
        props->setProp(FPformat, FFTtext[type]);
        props->setProp(FPkind, FFTtext[FFTrecfmv]);
        break;
    default:
        PROGLOG("unknown type %d",(int)type);
        throwError(DFTERR_UnknownFormatType);
    }
}

void FileFormat::serialize(MemoryBuffer & out) const
{
    out.append((byte)type);
    switch (type)
    {
    case FFTfixed:
    case FFTblocked:
        out.append(recordSize);
        break;
    case FFTcsv:
    case FFTutf:
    case FFTutf8: case FFTutf8n:
    case FFTutf16: case FFTutf16be: case FFTutf16le:
    case FFTutf32: case FFTutf32be: case FFTutf32le:
        out.append(maxRecordSize);
        ::serialize(out, separate);
        ::serialize(out, quote);
        ::serialize(out, terminate);
        ::serialize(out, rowTag);
        break;
    }
}

void FileFormat::serializeExtra(MemoryBuffer & out, unsigned version) const
{
    switch (type)
    {
    case FFTcsv:
    case FFTutf:
    case FFTutf8: case FFTutf8n:
    case FFTutf16: case FFTutf16be: case FFTutf16le:
    case FFTutf32: case FFTutf32be: case FFTutf32le:
        if (version == 1)
            ::serialize(out, escape);
        break;
    }
}

void FileFormat::set(const FileFormat & src)
{
    type = src.type;
    recordSize = src.recordSize;
    maxRecordSize = src.maxRecordSize;
    separate.set(src.separate);
    quote.set(src.quote);
    terminate.set(src.terminate);
    escape.set(src.escape);
    rowTag.set(src.rowTag);
    quotedTerminator = src.quotedTerminator;
    markup = src.markup;
    headerLength = src.headerLength;
    footerLength = src.footerLength;
}


UtfReader::UtfFormat getUtfFormatType(FileFormatType type)
{
    switch (type)
    {
    case FFTutf:    return UtfReader::Utf8;
    case FFTutf8:   return UtfReader::Utf8;
    case FFTutf8n:  return UtfReader::Utf8;

    case FFTutf16:  return UtfReader::Utf16be;
    case FFTutf16be:return UtfReader::Utf16be;
    case FFTutf16le:return UtfReader::Utf16le;

    case FFTutf32:  return UtfReader::Utf32be;
    case FFTutf32be:return UtfReader::Utf32be;
    case FFTutf32le:return UtfReader::Utf32le;
    }
    return UtfReader::Utf8;
}

bool sameEncoding(const FileFormat & src, const FileFormat & tgt)
{
    if (src.equals(tgt))
        return true;
    switch (src.type)
    {
    case FFTutf8n:
        return (tgt.type == FFTutf8);
    case FFTutf16be:
        return (tgt.type == FFTutf16);
    case FFTutf32be:
        return (tgt.type == FFTutf32);
    }
    return false;
}


//----------------------------------------------------------------------------

const char * getHeaderText(FileFormatType type)
{
    switch (type)
    {
    case FFTutf:
    case FFTutf8:
        return "\xEF\xBB\xBF";
    case FFTutf16:
        return "\xFE\xFF";
    case FFTutf32:
        return "\x00\x00\xFE\xFF";
    }
    return NULL;
}

unsigned getHeaderSize(FileFormatType type)
{
    const char * headerText = getHeaderText(type);
    return headerText ? strlen(headerText) : 0;
}

//---------------------------------------------------------------------------

OutputProgress::OutputProgress()
{
    whichPartition = (unsigned)-1;
    hasInputCRC = false;
    reset();
}

void OutputProgress::reset()
{
    status = StatusBegin;
    inputCRC = 0;
    inputLength = 0;
    outputCRC = 0;
    outputLength = 0;
    hasCompressed = false;
    compressedPartSize = 0;
}

MemoryBuffer & OutputProgress::deserializeCore(MemoryBuffer & in)
{
    unsigned _inputCRC, _outputCRC;
    bool hasTime;
    in.read(status).read(whichPartition).read(hasInputCRC).read(_inputCRC).read(inputLength).read(_outputCRC).read(outputLength).read(hasTime);
    inputCRC = _inputCRC;
    outputCRC = _outputCRC;
    if (hasTime)
        resultTime.deserialize(in);
    else
        resultTime.clear();
    return in;
}

MemoryBuffer & OutputProgress::deserializeExtra(MemoryBuffer & in, unsigned version)
{
    if (in.remaining())
    {
        switch (version)
        {
        case 1:
            in.read(hasCompressed);
            if (hasCompressed)
                in.read(compressedPartSize);
            break;
        }
    }
    return in;
}


static const char * const statusText[] = {"Init","Active","Copied","Renamed"};
void OutputProgress::trace()
{
    LOG(MCdebugInfoDetail, unknownJob, "Chunk %d status: %s  input length: %" I64F "d[CRC:%x] -> output length:%" I64F "d[CRC:%x]", whichPartition, statusText[status], inputLength, inputCRC, outputLength, outputCRC);
}

MemoryBuffer & OutputProgress::serializeCore(MemoryBuffer & out)
{
    bool hasTime = !resultTime.isNull();
    unsigned _inputCRC = inputCRC;
    unsigned _outputCRC = outputCRC;
    out.append(status).append(whichPartition).append(hasInputCRC).append(_inputCRC).append(inputLength).append(_outputCRC).append(outputLength).append(hasTime);
    if (hasTime)
        resultTime.serialize(out);
    return out;
}

MemoryBuffer & OutputProgress::serializeExtra(MemoryBuffer & out, unsigned version)
{
    switch (version)
    {
    case 1:
        out.append(hasCompressed);
        if (hasCompressed )
            out.append(compressedPartSize);
        break;
    }
    return out;
}

void OutputProgress::set(const OutputProgress & other)
{
    whichPartition = other.whichPartition;
    hasInputCRC = other.hasInputCRC;
    inputCRC = other.inputCRC;
    inputLength = other.inputLength;
    outputCRC = other.outputCRC;
    outputLength = other.outputLength;
    status = other.status;
    resultTime = other.resultTime;
    hasCompressed = other.hasCompressed;
    compressedPartSize = other.compressedPartSize;
}

void OutputProgress::restore(IPropertyTree * tree)
{
    status = tree->getPropInt("@status");
    whichPartition = tree->getPropInt("@partition");
    hasInputCRC = tree->hasProp("@inputCRC");
    inputCRC = tree->getPropInt("@inputCRC", 0);
    inputLength = tree->getPropInt64("@inputLength");
    outputCRC = tree->getPropInt("@outputCRC");
    outputLength = tree->getPropInt64("@outputLength");
    resultTime.setString(tree->queryProp("@modified"));
    hasCompressed = tree->getPropBool("@compressed");
    compressedPartSize = tree->getPropInt64("@compressedPartSize");
}

void OutputProgress::save(IPropertyTree * tree)
{
    tree->setPropInt("@status", status);
    tree->setPropInt("@partition", whichPartition);
    if (hasInputCRC)
        tree->setPropInt("@inputCRC", inputCRC);
    tree->setPropInt64("@inputLength", inputLength);
    tree->setPropInt("@outputCRC", outputCRC);
    tree->setPropInt64("@outputLength", outputLength);
    if (!resultTime.isNull())
    {
        StringBuffer timestr;
        tree->setProp("@modified", resultTime.getString(timestr));
    }
    tree->setPropInt("@compressed", hasCompressed);
    tree->setPropInt64("@compressedPartSize", compressedPartSize);
}


void displayProgress(OutputProgressArray & progress)
{
    LOG(MCdebugInfoDetail, unknownJob, "Progress:");
    ForEachItemIn(idx, progress)
        progress.item(idx).trace();
}

//---------------------------------------------------------------------------

void displayPartition(PartitionPointArray & partition)
{
    LOG(MCdebugInfoDetail, unknownJob, "Partition:");
    ForEachItemIn(idx, partition)
        partition.item(idx).display();
}

void deserialize(PartitionPointArray & partition, MemoryBuffer & in)
{
    unsigned count;
    in.read(count);
    for (unsigned idx = 0; idx < count; idx++)
    {
        PartitionPoint & next = * new PartitionPoint;
        next.deserialize(in);
        partition.append(next);
    }
}

void serialize(PartitionPointArray & partition, MemoryBuffer & out)
{
    out.append(partition.ordinality());
    ForEachItemIn(idx, partition)
        partition.item(idx).serialize(out);
}

//---------------------------------------------------------------------------

CrcIOStream::CrcIOStream(IFileIOStream * _stream, unsigned startCRC)
{
    stream.set(_stream);
    crc = startCRC;
}


void CrcIOStream::flush()
{
}

size32_t CrcIOStream::read(size32_t len, void * data)
{
    size32_t got = stream->read(len, data);
    crc = crc32((const char *)data, got, crc);
    return got;
}

void CrcIOStream::seek(offset_t pos, IFSmode origin)
{
    stream->seek(pos, origin);
    //MORE - no sensible thing to do on a seek....
}

offset_t CrcIOStream::size()
{
    return stream->size();
}

offset_t CrcIOStream::tell()
{
    return stream->tell();
}

size32_t CrcIOStream::write(size32_t len, const void * data)
{
    size32_t written = stream->write(len, data);
    crc = crc32((const char *)data, written, crc);
    return written;
}

//---------------------------------------------------------------------------

static int breakCount;
bool daftAbortHandler()
{
    LOG(MCuserProgress, unknownJob, "Aborting...");
    // hit ^C 3 times to really stop it...
    if (breakCount++ >= 2)
    {
        closeEnvironment();
        return true;
    }
    return false;
}

//---------------------------------------------------------------------------

const char * queryFtSlaveExecutable(const IpAddress &ip, StringBuffer &ret)
{
    StringBuffer dir; // not currently used
    return querySlaveExecutable("FTSlaveProcess", "ftslave", NULL, ip, ret, dir);
}

static StringAttr ftslavelogdir;

const char * queryFtSlaveLogDir()
{
    return ftslavelogdir.get();
}

void setFtSlaveLogDir(const char *dir)
{
    PROGLOG("ftslave log dir set to %s",dir);
    ftslavelogdir.set(dir);
}

