/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#include "xslcache.hpp"

class CXslIncludeSignature : public CInterface, implements IInterface
{
private:
    StringBuffer filePath;
    offset_t fileSize;
    time_t fileTime;

public:
    IMPLEMENT_IINTERFACE;

    CXslIncludeSignature(const char* path)
    {
        if(!path || !*path)
            throw MakeStringException(-1, "CXslIncludeSignature : path can't be emtpy");
        filePath.append(path);
        Owned<IFile> f = createIFile(path);
        if(f)
        {
            CDateTime modtime;
            f->getTime(NULL, &modtime, NULL);
            fileTime = modtime.getSimple();
            fileSize = f->size();
        }
        else
        {
            fileSize = 0;
            fileTime = 0;
        }
    }

    bool operator==(CXslIncludeSignature& incl)
    {
        return (streq(filePath.str(), incl.filePath.str())) && (fileSize == incl.fileSize) && (fileTime == incl.fileTime);
    }

    const char* getPath()
    {
        return filePath.str();
    }
};

class CXslIncludeCompare : public CInterface, implements IInterface
{
private:
    IArrayOf<CXslIncludeSignature> m_signatures;

public:
    IMPLEMENT_IINTERFACE;

    CXslIncludeCompare(StringArray& includepaths)
    {
        ForEachItemIn(x, includepaths)
        {
            const char* path = includepaths.item(x);
            if(path && *path)
                m_signatures.append(*(new CXslIncludeSignature(path)));
        }
    }

    virtual ~CXslIncludeCompare()
    {
    }

    bool hasChanged()
    {
        ForEachItemIn(x, m_signatures)
        {
            CXslIncludeSignature &compiled = m_signatures.item(x);
            CXslIncludeSignature current(compiled.getPath());
            if(!(compiled == current))
                return true;
        }
        return false;
    }
};

class CXslEntry : public CInterface, implements IInterface
{
public:
    Owned<IXslBuffer> m_buffer;
    offset_t m_size;
    time_t createTime;
    time_t fileModTime;
    Owned<CXslIncludeCompare> m_includecomp;
    StringAttr m_cacheId;
public:
    IMPLEMENT_IINTERFACE;

    CXslEntry(IXslBuffer* buffer)
    {
        assertex(buffer);
        m_size = 0;
        fileModTime = 0;
        time(&createTime);

        m_buffer.set(buffer);
        m_cacheId.set(buffer->getCacheId());

        if(m_buffer->getType() == IO_TYPE_FILE)
        {
            Owned<IFile> f = createIFile(buffer->getFileName());
            if(f)
            {
                CDateTime modtime;
                f->getTime(NULL, &modtime, NULL);
                fileModTime = modtime.getSimple();
                m_size = f->size();
            }
        }
        else
            m_size = buffer->getLen();
    }

    IXslBuffer* getBuffer()
    {
        return m_buffer.get();
    }

    bool isExpired(int cacheTimeout)
    {
        if (cacheTimeout==-1)
            return false;
        time_t t;
        time(&t);
        if(t > createTime + cacheTimeout)
            return true;
        return false;
    }

    bool match(CXslEntry* entry)
    {
        if(!entry)
            return false;
        if(m_buffer->getType() != entry->m_buffer->getType())
            return false;
        if (!m_cacheId.length())
            return false;
        return (entry->m_cacheId.length()) && (streq(m_cacheId.str(), entry->m_cacheId.str()));
    }

    bool equal(CXslEntry* entry, bool matched)
    {
        if(!entry)
            return false;
        if(!matched && !match(entry))
            return false;
        if (fileModTime != entry->fileModTime || m_size != entry->m_size)
            return false;
        if(m_includecomp.get() && m_includecomp->hasChanged())
            return false;
        return true;
    }

    void compile()
    {
        if(m_buffer.get())
        {
            m_buffer->compile();
            if(m_buffer->getIncludes().length())
                m_includecomp.setown(new CXslIncludeCompare(m_buffer->getIncludes()));
        }
    }
};

#define XSLTCACHESIZE 256

class CXslCache : public CInterface, implements IXslCache
{
private:
    MapStringToMyClass<CXslEntry> xslMap;
    Mutex m_mutex;
    int m_cachetimeout;

public:
    IMPLEMENT_IINTERFACE;
    
    CXslCache()
    {
        m_cachetimeout = XSLT_DEFAULT_CACHETIMEOUT;
    }

    virtual ~CXslCache()
    {
    }

    IXslBuffer* getCompiledXsl(IXslBuffer* xslbuffer, bool replace)
    {
        if(!xslbuffer)
            return NULL;

        synchronized block(m_mutex);

        Owned<CXslEntry> newEntry = new CXslEntry(xslbuffer);
        const char *cacheId = newEntry->m_cacheId.get();
        if (cacheId && *cacheId)
        {
            CXslEntry* cacheEntry = xslMap.getValue(cacheId);
            if (cacheEntry)
            {
                if(!replace && !cacheEntry->isExpired(m_cachetimeout) && cacheEntry->equal(newEntry, false))
                    return cacheEntry->getBuffer();
                else
                    xslMap.remove(cacheId);
            }
        }
        newEntry->compile();
        if (cacheId && *cacheId)
        {
            if (xslMap.count()>=XSLTCACHESIZE)
                freeOneCacheEntry();
            xslMap.setValue(cacheId, newEntry.get());
        }
        return xslbuffer;
    }

    void freeOneCacheEntry()
    {
        IMapping *oldest = NULL;
        time_t oldTime = 0;
        HashIterator iter(xslMap);
        for (iter.first(); iter.isValid(); iter.next())
        {
            CXslEntry *entry = xslMap.mapToValue(&iter.query());
            if (entry->isExpired(m_cachetimeout))
            {
                xslMap.removeExact(& static_cast<MappingStringToIInterface &>(iter.query()));
                return;
            }
            if (!oldest || entry->createTime < oldTime)
            {
                oldest=&iter.query();
                oldTime=entry->createTime;
            }
        }
        if (oldest)
            xslMap.removeExact(static_cast<MappingStringToIInterface *>(oldest));
    }

    void setCacheTimeout(int timeout)
    {
        m_cachetimeout = timeout;
    }
};

IXslCache* getXslCache()
{
    static Owned<CXslCache> xslcache;
    if(!xslcache)
        xslcache.setown(new CXslCache());

    return xslcache.get();
}

IXslCache* getXslCache2()
{
    static Owned<CXslCache> xslcache2;
    if(!xslcache2)
        xslcache2.setown(new CXslCache());

    return xslcache2.get();
}
