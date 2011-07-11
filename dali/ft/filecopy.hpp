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

#ifndef FILECOPY_HPP
#define FILECOPY_HPP

#include "jsocket.hpp"
#include "dafdesc.hpp"
#include "daft.hpp"
#include "junicode.hpp"

typedef enum 
{ 
    FFTunknown, 
    FFTfixed, FFTvariable, FFTblocked, 
    FFTcsv,
    FFTutf,                             // any format, default to utf-8n
    FFTutf8, FFTutf8n,
    FFTutf16, FFTutf16be, FFTutf16le,
    FFTutf32, FFTutf32be, FFTutf32le,
    FFTrecfmvb, FFTrecfmv, FFTvariablebigendian,
    FFTlast
 } FileFormatType;
enum { FTactionpull, FTactionpush, FTactionpartition, FTactiondirectory, FTactionsize, FTactionpcopy };


#define EFX_BLOCK_SIZE          32768

class DALIFT_API FileFormat
{
public:
    FileFormat(FileFormatType _type = FFTunknown, unsigned _recordSize = 0) { set(_type, _recordSize); maxRecordSize = 0;}

    void deserialize(MemoryBuffer & in);
    bool equals(const FileFormat & other) const     { return (type == other.type) && (recordSize == other.recordSize); }
    unsigned getUnitSize() const;
    bool isUtf() const                              { return (type >= FFTutf) && (type <= FFTutf32le); }
    bool restore(IPropertyTree * props);
    void save(IPropertyTree * props);
    void serialize(MemoryBuffer & out) const;
    void set(FileFormatType _type, unsigned _recordSize = 0) { type = _type, recordSize = _recordSize; }
    void set(const FileFormat & src);

public:
    FileFormatType      type;
    unsigned            recordSize;
    unsigned            maxRecordSize;
    StringAttr          separate;
    StringAttr          quote;
    StringAttr          terminate;
    StringAttr          rowTag;
};
UtfReader::UtfFormat getUtfFormatType(FileFormatType type);
bool sameEncoding(const FileFormat & src, const FileFormat & tgt);

interface IFileSprayer : public IInterface
{
public:
    virtual void removeSource() = 0;
    virtual void setAbort(IAbortRequestCallback * _abort) = 0;
    virtual void setPartFilter(IDFPartFilter * _filter) = 0;
    virtual void setProgress(IDaftProgress * _progress) = 0;
    virtual void setReplicate(bool _replicate) = 0;
    virtual void setSource(IDistributedFile * source) = 0;
    virtual void setSource(IFileDescriptor * source) = 0;
    virtual void setSource(IDistributedFilePart * part) = 0;
    virtual void setSourceTarget(IFileDescriptor * fd, DaftReplicateMode mode) = 0;
    virtual void setTarget(IDistributedFile * target) = 0;
    virtual void setTarget(IFileDescriptor * target, unsigned copy=0) = 0;
    virtual void setTarget(IGroup * target) = 0;
    virtual void setTarget(INode * target) = 0;
    virtual void spray() = 0;
};

extern DALIFT_API IFileSprayer * createFileSprayer(IPropertyTree * _options, IPropertyTree * _progress, IRemoteConnection * recoveryConnection, const char *wuid);

extern DALIFT_API void testPartitions();

#endif
