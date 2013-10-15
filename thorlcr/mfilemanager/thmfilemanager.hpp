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

#ifndef _THMFILEMANAGER_HPP
#define _THMFILEMANAGER_HPP

#ifdef _WIN32
 #ifdef MFILEMANAGER_EXPORTS
  #define thmfilemanager_decl __declspec(dllexport)
 #else
  #define thmfilemanager_decl __declspec(dllimport)
 #endif
#else
 #define thmfilemanager_decl
#endif

#include "jarray.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jsocket.hpp"
#include "jtime.hpp"

#include "dafdesc.hpp"

#include "thormisc.hpp"

#define PAUSETMPSCOPE "thorpause"

class CJobBase;
interface IDistributedFile;
interface IThorFileManager : extends IInterface
{
    virtual void noteFileRead(CJobBase &job, IDistributedFile *file, bool extended=false) = 0;
    virtual IDistributedFile *lookup(CJobBase &job, const char *logicalName, bool temporary=false, bool optional=false, bool reportOptional=false, bool updateAccessed=true) = 0;
    virtual IFileDescriptor *create(CJobBase &job, const char *logicalName, StringArray &groupNames, IArrayOf<IGroup> &groups, bool overwriteok, unsigned helperFlags=0, bool nonLocalIndex=false, unsigned restrictedWidth=0) = 0;
    virtual void publish(CJobBase &job, const char *logicalName, bool mangle, IFileDescriptor &file, Owned<IDistributedFile> *publishedFile=NULL, unsigned partOffset=0, bool createMissingParts=true) = 0;
    virtual void clearCacheEntry(const char *name) = 0;
    virtual StringBuffer &mangleLFN(CJobBase &job, const char *lfn, StringBuffer &out) = 0;
    virtual StringBuffer &addScope(CJobBase &job, const char *logicalname, StringBuffer &ret, bool temporary=false, bool paused=false) = 0;
    virtual StringBuffer &getPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, StringBuffer &res) = 0;
    virtual StringBuffer &getPublishPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, StringBuffer &res) = 0;
    virtual unsigned __int64 getFileOffset(CJobBase &job, const char *logicalName, unsigned partno) = 0;
    virtual bool scanLogicalFiles(CJobBase &job, const char *_pattern, StringArray &results) = 0;
};

extern thmfilemanager_decl void initFileManager();
extern thmfilemanager_decl IThorFileManager &queryThorFileManager();
extern thmfilemanager_decl IFileDescriptor *getConfiguredFileDescriptor(IDistributedFile &file);
extern thmfilemanager_decl unsigned getGroupOffset(IGroup &fileGroup, IGroup &group);
extern thmfilemanager_decl void fillClusterArray(CJobBase &job, const char *filename, StringArray &clusters, IArrayOf<IGroup> &groups);

#endif
