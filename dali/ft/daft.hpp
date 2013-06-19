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

#ifndef DAFT_HPP
#define DAFT_HPP

#ifdef _WIN32
#ifdef DALIFT_EXPORTS
#define DALIFT_API __declspec(dllexport)
#else
#define DALIFT_API __declspec(dllimport)
#endif
#else
#define DALIFT_API
#endif

#include "dadfs.hpp"

interface IFile;
interface IMultiException;

interface IDaftProgress
{
    virtual void onProgress(unsigned __int64 sizeDone, unsigned __int64 totalSize, unsigned numNodes) = 0;          // how much has been done
    virtual void setRange(unsigned __int64 sizeReadBefore, unsigned __int64 totalSize, unsigned totalNodes) = 0;          // how much has been done
};

interface IDaftCopyProgress
{
    virtual void onProgress(const char * source) = 0;          // which file is being copied
};

enum DaftReplicateMode
{
    DRMreplicatePrimary,
    DRMreplicateSecondary, 
    DRMcreateMissing
};

interface IRemoteConnection;
interface IAbortRequestCallback;
interface IDistributedFileSystem : public IInterface
{
//operations on multiple files.
    virtual void copy(IDistributedFile * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;
    virtual void exportFile(IDistributedFile * from, IFileDescriptor * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;
    virtual void import(IFileDescriptor * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;
    virtual void move(IDistributedFile * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;
    virtual void replicate(IDistributedFile * from, IGroup *destgroup, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;        // create new set of copies assumes partname and dir location same as src (only nodes differ) will raise exception if nodes clash
    virtual void replicate(IFileDescriptor * fd, DaftReplicateMode mode, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;  // create new set of copies (between copy 0 to copy 1 depending on the mode) @crc set in options to copy if crc differs @sizedate if size/date differ.
    virtual void transfer(IFileDescriptor * from, IFileDescriptor * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress , IAbortRequestCallback * abort=NULL , const char *wuid=NULL) = 0;       // copy between external files, must have 

    virtual void directory(const char * directory, IGroup * machines, IPropertyTree * options, IPropertyTree * result) = 0;                 // @recurse=false @time=true @crc=false
    virtual void physicalCopy(const char * source, const char * target, IPropertyTree * options, IDaftCopyProgress * progress = NULL) = 0;      // options can include @recurse @copyMissing @copyExisting @preserveTimes @preserveIfNewer @verboseFull @verbose
    virtual void physicalCopy(IPropertyTree * source, const char * target, IPropertyTree * options, IDaftCopyProgress * progress = NULL) = 0;   // property tree is same structure as result of directory

//operations on a single file.
    virtual offset_t getSize(IDistributedFile * file,
                             bool forceget=false,                               // if true gets physical size (ignores cached attribute)
                             bool dontsetattr=true) = 0;                        // if true doesn't set attribute when physical size got
    virtual bool compress(IDistributedFile * file) = 0;                                                  
    virtual offset_t getCompressedSize(IDistributedFile * part) = 0;

//operations on a file part
    virtual IFile *getIFile(IDistributedFilePart * part, unsigned copy=0) = 0;  // get IFile for reading/writing file part
    virtual offset_t getSize(IDistributedFilePart * part,
                             bool forceget=false,                               // if true gets physical size (ignores cached attribute)
                             bool dontsetattr=true) = 0;                        // if true doesn't set attribute when physical size got
    virtual void replicate(IDistributedFilePart * part, INode *node) = 0;       // creates single copy
    virtual bool compress(IDistributedFilePart * part) = 0;   
    virtual offset_t getCompressedSize(IDistributedFilePart * part) = 0;
};

extern DALIFT_API IDistributedFileSystem & queryDistributedFileSystem();

extern DALIFT_API const char * queryFtSlaveLogDir();
extern DALIFT_API void setFtSlaveLogDir(const char *dir);

#endif
