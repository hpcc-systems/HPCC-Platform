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

#ifndef DAFT_IPP
#define DAFT_IPP

#include "daft.hpp"


class CDistributedFileSystem : public CInterface, implements IDistributedFileSystem
{
public:
    CDistributedFileSystem();
    IMPLEMENT_IINTERFACE

//operations on multiple files.
    virtual void copy(IDistributedFile * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);
    virtual void exportFile(IDistributedFile * from, IFileDescriptor * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);
    virtual void import(IFileDescriptor * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);
    virtual void move(IDistributedFile * from, IDistributedFile * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);
    virtual void replicate(IDistributedFile * from, IGroup *destgroup, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);
    virtual void replicate(IFileDescriptor * fd, DaftReplicateMode mode, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);
    virtual void transfer(IFileDescriptor * from, IFileDescriptor * to, IPropertyTree * recovery, IRemoteConnection * recoveryConnection, IDFPartFilter *filter, IPropertyTree * options, IDaftProgress * progress, IAbortRequestCallback * abort, const char *wuid);

    virtual void directory(const char * directory, IGroup * machines, IPropertyTree * options, IPropertyTree * result);
    virtual void physicalCopy(const char * source, const char * target, IPropertyTree * options, IDaftCopyProgress * progress);
    virtual void physicalCopy(IPropertyTree * source, const char * target, IPropertyTree * options, IDaftCopyProgress * progress);

//operations on a single file.
    virtual offset_t getSize(IDistributedFile * file,bool forceget,bool dontsetattr);
    virtual bool compress(IDistributedFile * file);                                                  
    virtual offset_t getCompressedSize(IDistributedFile * part);

//operations on a file part
    virtual IFile *getIFile(IDistributedFilePart * part, unsigned copy);
    virtual offset_t getSize(IDistributedFilePart * part,bool forceget,bool dontsetattr);
    virtual void replicate(IDistributedFilePart * part, INode *node);
    virtual bool compress(IDistributedFilePart * part);                                                  
    virtual offset_t getCompressedSize(IDistributedFilePart * part);
};

#endif
