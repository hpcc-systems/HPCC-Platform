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
    virtual IDistributedFile *lookup(CJobBase &job, const char *logicalName, bool temporary=false, bool optional=false, bool reportOptional=false) = 0;
    virtual IFileDescriptor *create(CJobBase &job, const char *logicalName, StringArray &groupNames, IArrayOf<IGroup> &groups, bool overwriteok, unsigned helperFlags=0, bool nonLocalIndex=false, unsigned restrictedWidth=0) = 0;
    virtual void publish(CJobBase &job, const char *logicalName, bool mangle, IFileDescriptor &file, Owned<IDistributedFile> *publishedFile=NULL, unsigned partOffset=0, bool createMissingParts=true) = 0;
    virtual void clearCacheEntry(const char *name) = 0;
    virtual StringBuffer &mangleLFN(CJobBase &job, const char *lfn, StringBuffer &out) = 0;
    virtual StringBuffer &addScope(CJobBase &job, const char *logicalname, StringBuffer &ret, bool temporary=false, bool paused=false) = 0;
    virtual StringBuffer &getPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, StringBuffer &res) = 0;
    virtual StringBuffer &getPublishPhysicalName(CJobBase &job, const char *logicalName, unsigned partno, StringBuffer &res) = 0;
    virtual unsigned __int64 getFileOffset(CJobBase &job, const char *logicalName, unsigned partno) = 0;
    virtual void updateAccessTime(CJobBase &job, const char *logicalName) = 0;
    virtual bool scanLogicalFiles(CJobBase &job, const char *_pattern, StringArray &results) = 0;
};

extern thmfilemanager_decl void initFileManager();
extern thmfilemanager_decl IThorFileManager &queryThorFileManager();
extern thmfilemanager_decl IFileDescriptor *getConfiguredFileDescriptor(IDistributedFile &file);
extern thmfilemanager_decl unsigned getGroupOffset(IGroup &fileGroup, IGroup &group);
extern thmfilemanager_decl void fillClusterArray(CJobBase &job, const char *filename, StringArray &clusters, IArrayOf<IGroup> &groups);

#endif
