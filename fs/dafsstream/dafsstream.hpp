/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#ifndef DAFSCLIENT_HPP
#define DAFSCLIENT_HPP

#ifdef DAFSCLIENT_EXPORTS
#define DAFSCLIENT_API DECL_EXPORT
#else
#define DAFSCLIENT_API DECL_IMPORT
#endif

#include "seclib.hpp"


interface IOutputMetaData;
namespace dafsstream
{

enum DFUFileType { dft_none, dft_flat, dft_index, dft_csv, dft_xml, dft_json };

enum DaFsExceptionCode
{
    DaFsClient_NoStreamWriteSupport = 1,
    DaFsClient_OpenFailure,
    DaFsClient_ECLParseError,
    DaFsClient_ConnectionFailure,
    DaFsClient_TooOld,
    DaFsClient_NotStarted,
    DaFsClient_ReaderEndOfStream,
    DaFsClient_CompressorSetupError,
    DaFsClient_InvalidFileType,
    DaFsClient_WriteError,
    DaFsClient_InvalidMetaInfo,
};

interface IDaFsException : extends IException
{
};

interface DAFSCLIENT_API IDFUFilePartBase : extends IInterface
{
    virtual void start() = 0;
    virtual void finalize() = 0;
    virtual IOutputMetaData *queryMeta() const = 0;
};

interface IFPosStream : extends IInterface
{
    virtual bool next(offset_t &fpos) = 0;
    virtual bool peekAvailable() const = 0;
};
interface DAFSCLIENT_API IDFUFilePartReader : extends IDFUFilePartBase
{
    virtual const void *nextRow(size32_t &sz) = 0;
    virtual const void *getRows(size32_t min, size32_t &got) = 0; // will read at least min, and returned data will contain whole rows only

// NB: these methods should be called before start()
    virtual void addFieldFilter(const char *filter) = 0;
    virtual void clearFieldFilters() = 0;
    virtual void setOutputRecordFormat(const char *eclRecDef) = 0;
    virtual void addVirtualFieldMapping(const char *fieldName, const char *fieldValue) = 0;
    virtual void setFetchStream(IFPosStream *fposStream) = 0;
};

interface DAFSCLIENT_API IDFUFilePartWriter : extends IDFUFilePartBase
{
    virtual void write(size32_t sz, const void *rowData) = 0; // NB: can be multiple rows
};


interface DAFSCLIENT_API IDFUFileAccessExt : extends IInterface
{
    virtual IOutputMetaData *queryMeta() const = 0;
    virtual IFileDescriptor &queryFileDescriptor() const = 0;
    virtual IPropertyTree &queryProperties() const = 0; // NB: properties of file descriptor
    virtual void setLayoutBin(size32_t sz, const void *layoutBin) = 0;
};

enum DFUFileOption { dfo_null=0, dfo_compressedRemoteStreams=1 }; // NB: will be used in bit field
interface DAFSCLIENT_API IDFUFileAccess : extends IInterface
{
    virtual const char *queryName() const = 0;
    virtual const char *queryFileId() const = 0;
    virtual unsigned queryNumParts() const = 0;
    virtual SecAccessFlags queryAccessType() const = 0;
    virtual bool queryIsGrouped() const = 0;
    virtual DFUFileType queryType() const = 0;
    virtual bool queryIsCompressed() const = 0;
    virtual const char *queryClusterGroupName() const = 0;
    virtual const char *queryPartHost(unsigned part, unsigned copy=0) const = 0;
    virtual const char *queryJSONTypeInfo() const = 0;
    virtual const char *queryECLRecordDefinition() const = 0;
    virtual const void *queryMetaInfoBlob(size32_t &sz) const = 0;
    virtual const char *queryCommCompressionType() const = 0;
    virtual DFUFileOption queryFileOptions() const = 0;
    virtual bool isFileOptionSet(DFUFileOption opt) const = 0;
    virtual const char *queryFileProperty(const char *name) const = 0;
    virtual __int64 queryFilePropertyInt(const char *name) const = 0;
    virtual const char *queryPartProperty(unsigned part, const char *name) const = 0;
    virtual __int64 queryPartPropertyInt(unsigned part, const char *name) const = 0;

    virtual void setCommCompressionType(const char *compType) = 0;
    virtual void setFileOption(DFUFileOption opt) = 0;
    virtual void clearFileOption(DFUFileOption opt) = 0;
    virtual void setECLRecordDefinition(const char *eclRecDef) = 0;
    virtual void setFileProperty(const char *name, const char *value) = 0;
    virtual void setFilePropertyInt(const char *name, __int64 value) = 0;
    virtual void setPartProperty(unsigned part, const char *name, const char *value) = 0;
    virtual void setPartPropertyInt(unsigned part, const char *name, __int64 value) = 0;

// NB: these changes effect future creation of IDFUFilePartReader or IDFUFilePartWriter instances
    virtual void setStreamReplyLimitK(unsigned k) = 0;
    virtual void setExpirySecs(unsigned secs) = 0;
    virtual void setOption(const char *key, const char *value) = 0;

// NB: the intention is for a IDFUFileAccess to be used to create instances for multiple parts, but not to mix types.
    virtual IDFUFilePartReader *createFilePartReader(unsigned p, unsigned copy=0, IOutputMetaData *outMeta=nullptr, bool preserveGrouping=false) = 0;
    virtual IDFUFilePartWriter *createFilePartWriter(unsigned p) = 0;
    virtual IDFUFilePartWriter *createFilePartAppender(unsigned p) = 0;

    virtual IDFUFileAccessExt *queryEngineInterface() = 0;
};

// NB: fileId, supplied/only needed by older esp's at publish time
DAFSCLIENT_API IDFUFileAccess *createDFUFileAccess(const char *metaInfoBlobB64, const char *fileId=nullptr);
DAFSCLIENT_API IRowWriter *createRowWriter(IDFUFilePartWriter *partWriter);

DAFSCLIENT_API void setDefaultCommCompression(const char *compType);

} // end of namespace dafsstream

#endif
