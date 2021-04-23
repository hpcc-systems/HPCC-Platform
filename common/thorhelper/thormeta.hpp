/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#ifndef __THORMETA_HPP_
#define __THORMETA_HPP_

#ifdef THORHELPER_EXPORTS
 #define THORHELPER_API DECL_EXPORT
#else
 #define THORHELPER_API DECL_IMPORT
#endif

#include "jrowstream.hpp"
#include "rtlkey.hpp"
#include <vector>
#include "thorstore.hpp"


interface IDistributedFile;

//--------------------------------------------------------------------------------------------------------------------


//This class represents a logical file part.
//The superPartNum/baseOffset may not match the values for the file if this file is part of a superfile.
class CLogicalFilePart
{
public:
    CLogicalFilePart() = default;
    CLogicalFilePart(unsigned _superPartNum, offset_t _numRows, offset_t _fileSize, offset_t _baseOffset)
    : superPartNum(_superPartNum), numRows(_numRows), fileSize(_fileSize), baseOffset(_baseOffset)
    {
    }

public://should be private
    unsigned superPartNum;
    offset_t numRows = 0;
    offset_t fileSize = 0;
    offset_t baseOffset = 0; // sum of previous file sizes
//  Owned<CSplitPointTable> splits;   // This is where split points would be saved and accessed
};

class THORHELPER_API CLogicalFile : public CInterface
{
public:
    CLogicalFile(const CStorageSystems & storage, const IPropertyTree * xml, IOutputMetaData * _expectedMeta);

    StringBuffer & getURL(StringBuffer & target, unsigned part, unsigned copy) const;
    offset_t getFileSize() const { return fileSize; }
    unsigned getNumCopies() const;
    unsigned getNumParts() const { return numParts; }
    offset_t getPartSize(unsigned part) const;
    bool isDistributed() const { return numParts > 1; }  // MORE: Only if originally a logical file...
    bool isExternal() const { return metaTree->getPropBool("@external"); }
    bool isGrouped() const { return metaTree->getPropBool("@grouped"); }
    bool isLogicalFile() const { return name != nullptr; }
    bool isLocal(unsigned part, unsigned copy) const;
    bool isMissing() const { return metaTree->getPropBool("@missing"); }
    bool onAttachedStorage(unsigned copy) const;
    const CStoragePlane * queryPlane(unsigned idx) const { return planes.item(idx); }

    unsigned queryActualCrc() const { return actualCrc; }
    IOutputMetaData * queryActualMeta() const;
    const char * queryFormat() const;
    const IPropertyTree * queryFileMeta() const;
    const char * queryLogicalFilename() const;
    offset_t queryOffsetOfPart(unsigned part) const;
    const CLogicalFilePart & queryPart(unsigned part) const
    {
        assertex(part < parts.size());
        return parts[part];
    }
    StringBuffer & getTracingFilename(StringBuffer & out, unsigned part) const;

    const char * queryPhysicalPath() const { UNIMPLEMENTED; }    // MORE!!!
    bool includePartSuffix() const;

    void applyHelperOptions(const IPropertyTree * helperOptions);
    void noteLocation(unsigned part, unsigned superPartNum, offset_t baseOffset);

protected:
    StringBuffer & expandLogicalAsPhysical(StringBuffer & target, unsigned copy) const;
    StringBuffer & expandPath(StringBuffer & target, unsigned part, unsigned copy) const;
    const char * queryScopeSeparator(unsigned copy) const;

private:
    const IPropertyTree * metaTree = nullptr;
    Owned<const IPropertyTree> mergedMeta;
    IOutputMetaData * expectedMeta = nullptr;  // same as CLogicalFileCollection::expectedMeta
    //All of the following are derived from the xml
    const char * name = nullptr;
    unsigned numParts = 0;
    offset_t fileSize = 0;
    std::vector<CLogicalFilePart> parts;
    ConstPointerArrayOf<CStoragePlane> planes; // An array of locations the file is stored.  replicas are expanded out.
    mutable Owned<IOutputMetaData> actualMeta;
    mutable unsigned actualCrc = 0;
};

//This class is the unit that is passed to the disk reading classes to represent a section from a filepart.
class THORHELPER_API CLogicalFileSlice
{
    friend class CLogicalFileCollection;
public:
    CLogicalFileSlice(CLogicalFile * _file, unsigned _part, offset_t _startOffset = 0, offset_t _length = unknownFileSize);

    StringBuffer & getURL(StringBuffer & url, unsigned copy) const { return file->getURL(url, part, copy); }

    unsigned getNumCopies() const { return file->getNumCopies(); }
    bool isEmpty() const { return !file || length == 0; }
    bool isLogicalFile() const { return file->isLogicalFile(); }
    bool isRemoteReadCandidate(unsigned copy) const;
    bool isWholeFile() const;
    bool isLocal(unsigned copy) const { return file->isLocal(part, copy); }
    bool onAttachedStorage(unsigned copy) const { return file->onAttachedStorage(copy); }

    CLogicalFile * queryFile() const { return file; }
    const char * queryFormat() const { return file->queryFormat(); }
    const IPropertyTree * queryFileMeta() const { return file->queryFileMeta(); }
    offset_t queryLength() const { return length; }
    unsigned queryPartNumber() const { return file->queryPart(part).superPartNum; }
    offset_t queryOffsetOfPart() const { return file->queryOffsetOfPart(part); }
    offset_t queryStartOffset() const { return startOffset; }
    const char * queryLogicalFilename() const { return file->queryLogicalFilename(); }
    StringBuffer & getTracingFilename(StringBuffer & out) const;

    void setAccessed() {}  // MORE:

private:
    CLogicalFileSlice() = default;

private:
    CLogicalFile * file = nullptr;
    unsigned part = 0;
    offset_t startOffset = 0;
    offset_t length = unknownFileSize;
    //MORE: What about HDFS records that are split over multiple files??
};

//MORE: Should this be a vector of owned pointers instead?
using SliceVector = std::vector<CLogicalFileSlice>;

class IFileCollectionContext
{
public:
    virtual void noteException(unsigned severity, unsigned code, const char * text) = 0;
};

//The following class is always used to access a collection of files - even if it is only a single physical file.
class CDfsLogicalFileName;
class THORHELPER_API CLogicalFileCollection
{
public:
    CLogicalFileCollection() = default;
    CLogicalFileCollection(MemoryBuffer & in);

    void init(IFileCollectionContext * _context, const char * _wuid,  bool _isTemporary,  bool _resolveLocally, bool _isCodeSigned, IUserDescriptor * _user, IOutputMetaData * _expectedMeta); // called once
    void calcPartition(SliceVector & slices, unsigned numChannels, unsigned channel, bool preserveDistribution, bool preserveOrder);
    void serialize(MemoryBuffer & out) const;
    void setEclFilename(const char * filename, IPropertyTree * helperOptions);
    void setTempFilename(const char * filename, IPropertyTree * helperOptions, const IPropertyTree * spillPlane);

protected:
    void appendFile(CLogicalFile & file);
    void applyHelperOptions();
    void calcLocations();
    void collectPartSlices(SliceVector & slices, unsigned part);
    void processFile(IDistributedFile * file, IOutputMetaData * expectedMeta, IPropertyTree * inputOptions, IPropertyTree * formatOptions);
    void processFilename(CDfsLogicalFileName & logicalFilename, IUserDescriptor *user, bool isTemporary, IOutputMetaData * expectedMeta, IPropertyTree * inputOptions, IPropertyTree * formatOptions);
    void processMissing(const char * filename, IPropertyTree * inputOptions);
    void processPhysicalFilename(const char * path, IOutputMetaData * expectedMeta, IPropertyTree * inputOptions, IPropertyTree * formatOptions);
    void processProtocolFilename(const char * name, const char * colon, const char * slash, IOutputMetaData * expectedMeta, IPropertyTree * inputOptions, IPropertyTree * formatOptions);
    void processLogicalFilename();
    void processResolvedMeta(IPropertyTree * _resolved);
    void reset();

private:
    //Options that are constant for the lifetime of the class - not linked because they are owned by something else.
    StringAttr wuid;
    IFileCollectionContext * context = nullptr;
    IUserDescriptor * user = nullptr;
    IOutputMetaData * expectedMeta = nullptr;
    bool isTemporary = false;
    bool isCodeSigned = false;
    bool resolveLocally = false;
    //The following may be reset e.g. if used within a child query
    StringAttr filename;
    Owned<IPropertyTree> helperOptions;    // defined by the helper functions
    //derived information
    Owned<IPropertyTree> resolved;
    CStorageSystems storageSystems;
    CIArrayOf<CLogicalFile> files;
    offset_t totalSize = 0;
    unsigned maxParts = 0;
};

#endif
