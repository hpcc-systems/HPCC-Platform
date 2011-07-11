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
    virtual bool remove(IDistributedFile * file,const char *cluster=NULL,IMultiException *mexcept=NULL);
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
