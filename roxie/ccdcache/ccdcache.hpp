/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef _CCDCACHE_INCL
#define _CCDCACHE_INCL

struct CacheInfoEntry
{
    //For convenience the values for PageType match the NodeX enumeration (see noteWarm).
    //Ensure disk entries sort last so that index nodes take precedence when deduping offsets.
    enum PageType : unsigned
    {
        PageTypeBranch = 0,
        PageTypeLeaf = 1,
        PageTypeBlob = 2,
        PageTypeDisk = 3,
    };

    union
    {
        struct
        {
#ifndef _WIN32
            unsigned type: 2;    // disk or the kind of index node
            __uint64 page: 38;   // Support file sizes up to 2^51 i.e. 2PB
            unsigned file: 24;   // Up to 4 million files
#else
//Windows does not like packing bitfields with different base types - fails the static assert
            __uint64 type: 2;    // disk or the kind of index node
            __uint64 page: 38;   // Support file sizes up to 2^51 i.e. 2PB
            __uint64 file: 24;   // Up to 16 million files
#endif
        } b;
        __uint64 u;
    };

#ifndef _WIN32
    static_assert(sizeof(b) == sizeof(u), "Unexpected packing issue in CacheInfoEntry");
#elif _MSC_VER >= 1900
    //Older versions of the windows compiler complain CacheInfoEntry::b is not a type name
    static_assert(sizeof(b) == sizeof(u), "Unexpected packing issue in CacheInfoEntry");
#endif

    inline CacheInfoEntry() { u = 0; }
    inline CacheInfoEntry(unsigned _file, offset_t _pos, PageType pageType)
    {
        b.file = _file;
        b.page = _pos >> pageBits;
        b.type = pageType;
    }
    inline bool operator < ( const CacheInfoEntry &l) const { return u < l.u; }
    inline bool operator <= ( const CacheInfoEntry &l) const { return u <= l.u; }
    inline void operator++ () { b.page++; }

    static constexpr unsigned pageBits = 13;  // 8k 'pages'
};

interface ICacheWarmer
{
    virtual void startFile(const char *filename) = 0;
    virtual bool warmBlock(const char *fileName, NodeType nodeType, offset_t startOffset, offset_t endOffset) = 0;
    virtual void endFile() = 0;
    virtual void report() = 0;
};

extern bool warmOsCache(const char *cacheInfo, ICacheWarmer *warmer);


#endif //_CCDCACHE_INCL
