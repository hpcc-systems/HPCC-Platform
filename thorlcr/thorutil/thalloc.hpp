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

#ifndef __THALLOC__
#define __THALLOC__

#ifdef _WIN32
    #ifdef GRAPH_EXPORTS
        #define graph_decl __declspec(dllexport)
    #else
        #define graph_decl __declspec(dllimport)
    #endif
#else
    #define graph_decl
#endif


interface IThorRowManager : extends IInterface
{
    virtual void *allocate(size32_t size, unsigned activityId) = 0;
    virtual void *clone(size32_t size, const void *source, unsigned activityId) = 0;
    virtual void *resizeRow(void * original, size32_t oldsize, size32_t newsize, unsigned activityId) = 0;
          // this is used to resize row to exact size (bigger or smaller)
    virtual void *finalizeRow(void *full, size32_t realsize, unsigned activityId, bool dup) = 0;
    virtual memsize_t allocated() = 0;
    virtual memsize_t remaining() = 0;
    virtual void reportLeaks() = 0;
    virtual void setLCRrowCRCchecking(bool on=true) = 0;
    // the following are used to extend row with slack
    virtual void *extendRow(void * original, size32_t newsize, unsigned activityId, size32_t &size) = 0;
    virtual void *allocateExt(size32_t size, unsigned activityId, size32_t &outsize) = 0;
};

interface IThorRowAllocatorCache
{
    virtual unsigned getActivityId(unsigned cacheId) const = 0;
    virtual StringBuffer &getActivityDescriptor(unsigned cacheId, StringBuffer &out) const = 0;
    virtual void onDestroy(unsigned cacheId, void *row) const = 0;
    virtual size32_t subSize(unsigned cacheId,const void *row) const = 0;
};

extern IThorRowManager *createThorRowManager(memsize_t memLimit, const IThorRowAllocatorCache *allocatorCache, bool ignoreLeaks = false);

// we are going to use 15 bit activity flag (index in cache)
#define MAX_ACTIVITY_ID 0x7fff
#define ACTIVITY_FLAG_ISREGISTERED  0  // Not used
#define ACTIVITY_FLAG_NEEDSDESTRUCTOR   0x8000

extern graph_decl void ReleaseThorRow(const void *ptr);
extern graph_decl void ReleaseClearThorRow(const void *&ptr);
extern graph_decl void LinkThorRow(const void *ptr);
extern graph_decl bool isThorRowShared(const void *ptr);
extern graph_decl void setThorRowCRC(const void *ptr);
extern graph_decl void clearThorRowCRC(const void *ptr);
extern graph_decl void checkThorRowCRC(const void *ptr);
extern graph_decl void logThorRow(const char *prefix,const void *row);
extern graph_decl size32_t thorRowMemoryFootprint(const void *ptr);



#endif
