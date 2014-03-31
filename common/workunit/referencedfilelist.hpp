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

#ifndef REFFILE_LIST_HPP
#define REFFILE_LIST_HPP

#include "jlib.hpp"
#include "workunit.hpp"
#include "package.h"
#include "dfuutil.hpp"

#define RefFileNone           0x000
#define RefFileIndex          0x001
#define RefFileNotOnCluster   0x002
#define RefFileNotFound       0x004
#define RefFileRemote         0x008
#define RefFileForeign        0x010
#define RefFileSuper          0x020
#define RefSubFile            0x040
#define RefFileCopyInfoFailed 0x080
#define RefFileCloned         0x100
#define RefFileInPackage      0x200
#define RefFileNotOnSource    0x400

interface IReferencedFile : extends IInterface
{
    virtual const char *getLogicalName() const =0;
    virtual unsigned getFlags() const =0;
    virtual const SocketEndpoint &getForeignIP(SocketEndpoint &ep) const =0;
    virtual const char *queryPackageId() const =0;
};

interface IReferencedFileIterator : extends IIteratorOf<IReferencedFile> { };

interface IReferencedFileList : extends IInterface
{
    virtual void addFilesFromWorkUnit(IConstWorkUnit *cw)=0;
    virtual void addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackageMap *pm, const char *queryid)=0;
    virtual void addFilesFromQuery(IConstWorkUnit *cw, const IHpccPackage *pkg)=0;
    virtual void addFilesFromPackageMap(IPropertyTree *pm)=0;

    virtual void addFile(const char *ln, const char *daliip=NULL, const char *sourceProcessCluster=NULL, const char *remotePrefix=NULL)=0;
    virtual void addFiles(StringArray &files)=0;

    virtual IReferencedFileIterator *getFiles()=0;
    virtual void resolveFiles(const char *process, const char *remoteIP, const char * remotePrefix, const char *srcCluster, bool checkLocalFirst, bool addSubFiles, bool resolveForeign=false)=0;
    virtual void cloneAllInfo(IDFUhelper *helper, bool overwrite, bool cloneSuperInfo, bool cloneForeign=false)=0;
    virtual void cloneFileInfo(IDFUhelper *helper, bool overwrite, bool cloneSuperInfo, bool cloneForeign=false)=0;
    virtual void cloneRelationships()=0;
};

extern WORKUNIT_API const char *skipForeign(const char *name, StringBuffer *ip=NULL);

extern WORKUNIT_API IReferencedFileList *createReferencedFileList(const char *user, const char *pw, bool allowForeignFiles);

extern WORKUNIT_API void splitDfsLocation(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix, const char *defaultCluster);
extern WORKUNIT_API void splitDerivedDfsLocation(const char *address, StringBuffer &cluster, StringBuffer &ip, StringBuffer &prefix, const char *defaultCluster, const char *baseCluster, const char *baseIP, const char *basePrefix);

#endif //REFFILE_LIST_HPP
