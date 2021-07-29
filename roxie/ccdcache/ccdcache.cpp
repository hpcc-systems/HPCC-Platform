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

#include "jlib.hpp"
#include "jfile.hpp"
#include "jhtree.hpp"
#include "ctfile.hpp"
#include "ccdfile.hpp"
#include "ccdcache.hpp"

#ifdef _STANDALONE_CCDCACHE
#if defined(__linux__) || defined(__APPLE__)
#include <sys/mman.h>
#endif
#include <setjmp.h>
#include <signal.h>
#endif

static unsigned __int64 readPage(const char * &_t)
{
    const char *t = _t;
    unsigned __int64 v = 0;
    for (;;)
    {
        char c = *t;
        if ((c >= '0') && (c <= '9'))
            v = v * 16 + (c-'0');
        else if ((c >= 'a') && (c <= 'f'))
            v = v * 16 + (c-'a'+10);
        else if ((c >= 'A') && (c <= 'F'))
            v = v * 16 + (c-'A'+10);
        else
            break;
        t++;
    }
    _t = t;
    return v;
}

// Note that warmOsCache is called twice for each cacheInfo file - once via separate process to touch pages into linux page cache,
// and once within the Roxie process to preload the index cache and initialize the cache info structure for future cache reports.

bool warmOsCache(const char *cacheInfo, ICacheWarmer *callback)
{
    if (!cacheInfo)
        return true;
    while (*cacheInfo)
    {
        // We are parsing lines that look like:
        // <channel>|<filename>|<pagelist>
        //
        // Where pagelist is a space-separated list of page numbers or (inclusive) ranges.
        // A page number or range prefixed by a * means that the page(s) was found in the jhtree cache.
        //
        // For example,
        // 1|/var/lib/HPCCSystems/hpcc-data/unknown/regress/multi/dg_index_evens._1_of_3|*0 3-4
        // Pages are always recorded and specified as 8192 bytes (unless pagebits ever changes).

        strtoul(cacheInfo, (char **) &cacheInfo, 10);  // Skip fileChannel - we don't care
        if (*cacheInfo != '|')
            break;
        cacheInfo++;
        const char *endName = strchr(cacheInfo, '|');
        assert(endName);
        if (!endName)
            break;
        StringBuffer fileName(endName-cacheInfo, cacheInfo);
        callback->startFile(fileName.str());
        cacheInfo = endName+1;  // Skip the |
        while (*cacheInfo==' ')
            cacheInfo++;
        for (;;)
        {
            bool inNodeCache = (*cacheInfo=='*');
            NodeType nodeType = NodeNone;
            if (inNodeCache)
            {
                cacheInfo++;
                switch (*cacheInfo)
                {
                case 'R': nodeType = NodeBranch; break;
                case 'L': nodeType = NodeLeaf; break;
                case 'B': nodeType = NodeBlob; break;
                default:
                    throwUnexpectedX("Unknown node type");
                }
                cacheInfo++;
            }
            __uint64 startPage = readPage(cacheInfo);
            __uint64 endPage;
            if (*cacheInfo=='-')
            {
                cacheInfo++;
                endPage = readPage(cacheInfo);
            }
            else
                endPage = startPage;
            offset_t startOffset = startPage << CacheInfoEntry::pageBits;
            offset_t endOffset = (endPage+1) << CacheInfoEntry::pageBits;
            if (!callback->warmBlock(fileName.str(), nodeType, startOffset, endOffset))
            {
                while (*cacheInfo && *cacheInfo != '\n')
                    cacheInfo++;
                break;
            }
            if (*cacheInfo != ' ')
                break;
            cacheInfo++;
        }
        callback->endFile();
        if (*cacheInfo != '\n')
            break;
        cacheInfo++;
    }
    assert(!*cacheInfo);
    return(*cacheInfo == '\0');
}

#ifdef _STANDALONE_CCDCACHE
// See example code at https://github.com/sublimehq/mmap-example/blob/master/read_mmap.cc

thread_local volatile bool sigbus_jmp_set;
thread_local sigjmp_buf sigbus_jmp_buf;

static void handle_sigbus(int c)
{
    // Only handle the signal if the jump point is set on this thread
    if (sigbus_jmp_set)
    {
        sigbus_jmp_set = false;

        // siglongjmp out of the signal handler, returning the signal
        siglongjmp(sigbus_jmp_buf, c);
    }
}

static void install_signal_handlers()
{
    // Install signal handler for SIGBUS
    struct sigaction act;
    act.sa_handler = &handle_sigbus;

    // SA_NODEFER is required due to siglongjmp
    act.sa_flags = SA_NODEFER;
    sigemptyset(&act.sa_mask); // Don't block any signals

    // Connect the signal
    sigaction(SIGBUS, &act, nullptr);
}

static bool testErrors = false;
static bool includeInCacheIndexes = false;
static size_t os_page_size = getpagesize();

class StandaloneCacheWarmer : implements ICacheWarmer
{
    unsigned traceLevel;
    unsigned filesTouched = 0;
    unsigned pagesTouched = 0;
    char *file_mmap = nullptr;
    int fd = -1;
    struct stat file_stat = {};
    char dummy = 0;

    void warmRange(offset_t startOffset, offset_t endOffset)
    {
        do
        {
            if (startOffset >= (offset_t) file_stat.st_size)
                break;    // Let's not core if the file has changed size since we recorded the info...
            dummy += file_mmap[startOffset];
            if (testErrors)
                raise(SIGBUS);
            pagesTouched++;
            startOffset += os_page_size;
        }
        while (startOffset < endOffset);
    }
public:
    StandaloneCacheWarmer(unsigned _traceLevel) : traceLevel(_traceLevel) {}

    virtual void startFile(const char *filename) override
    {
        file_mmap = nullptr;
        fd = open(filename, 0);
        if (fd != -1 && fstat(fd, &file_stat)==0)
        {
            file_mmap = (char *) mmap((void *)0, file_stat.st_size, PROT_READ, MAP_SHARED, fd, 0);
            if (file_mmap == MAP_FAILED)
            {
                printf("Failed to map file %s to pre-warm cache (error %d)\n", filename, errno);
                file_mmap = nullptr;
            }
            else
                filesTouched++;
        }
        else if (traceLevel)
        {
            printf("Failed to open file %s to pre-warm cache (error %d)\n", filename, errno);
        }
    }

    virtual bool warmBlock(const char *filename, NodeType nodeType, offset_t startOffset, offset_t endOffset) override
    {
        if (!includeInCacheIndexes && nodeType != NodeNone)
            return true;
        if (traceLevel > 8)
            printf("Touching %s %" I64F "x-%" I64F "x\n", filename, startOffset, endOffset);
        if (file_mmap)
        {
            sigbus_jmp_set = true;
            if (sigsetjmp(sigbus_jmp_buf, 0) == 0)
            {
                warmRange(startOffset, endOffset);
            }
            else
            {
                if (traceLevel)
                    printf("SIGBUF caught while trying to touch file %s at offset %" I64F "x\n", filename, startOffset);
                sigbus_jmp_set = false;
                return false;
            }
            sigbus_jmp_set = false;
            return true;
        }
        else
            return false;
    }

    virtual void endFile() override
    {
        if (file_mmap)
            munmap(file_mmap, file_stat.st_size);
        if (fd != -1)
            close(fd);
        fd = -1;
        file_mmap = nullptr;
    }

    virtual void report() override
    {
        if (traceLevel)
            printf("Touched %u pages from %u files (dummyval %u)\n", pagesTouched, filesTouched, dummy);  // We report dummy to make sure that compiler doesn't decide to optimize it away entirely
    }
};

static void usage()
{
    printf("Usage: ccdcache <options> filename\n");
    printf("Options:\n");
    printf("  -t  traceLevel\n");
    printf("  -i  Include in-cache index files too\n");
    exit(2);
}

int main(int argc, const char **argv)
{
    if (argc < 2)
        usage();
    int arg = 1;
    const char *cacheFileName = nullptr;
    unsigned traceLevel = 1;
    while (arg < argc)
    {
        if (streq(argv[arg], "-t") || streq(argv[arg], "--traceLevel"))
        {
            arg++;
            if (arg == argc)
                usage();
            traceLevel = atoi(argv[arg]);
        }
        else if (streq(argv[arg], "-e") || streq(argv[arg], "--testErrors"))
        {
            testErrors = true;
        }
        else if (streq(argv[arg], "-i") || streq(argv[arg], "--includecachedindexes"))
        {
            includeInCacheIndexes = true;
        }
        else if (*(argv[arg]) == '-' || cacheFileName != nullptr)
            usage();
        else
            cacheFileName = argv[arg];
        arg++;
    }
    if (!cacheFileName)
        usage();
    StringBuffer cacheInfo;
    install_signal_handlers();
    StandaloneCacheWarmer warmer(traceLevel);
    try
    {
        if (checkFileExists(cacheFileName))
        {
             if (traceLevel)
                printf("Loading cache information from %s\n", cacheFileName);
            cacheInfo.loadFile(cacheFileName, false);
            if (!warmOsCache(cacheInfo, &warmer))
                printf("WARNING: Unrecognized cacheInfo format in file %s\n", cacheFileName);
            warmer.report();
        }
    }
    catch(IException *E)
    {
        EXCLOG(E);
        E->Release();
    }
}
#endif

