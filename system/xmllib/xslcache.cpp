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

#include "xslcache.hpp"

class CXslIncludeSignature : public CInterface, implements IInterface
{
private:
    IO_Type m_type;
    StringBuffer m_path;

    int m_size;
    unsigned long m_modtime;
    unsigned long m_crc;

public:
    IMPLEMENT_IINTERFACE;

    CXslIncludeSignature(const char* path)
    {
        if(!path || !*path)
            throw MakeStringException(-1, "CXslIncludeSignature : path can't be emtpy");

        m_path.append(path);

        m_type = IO_TYPE_FILE;
        Owned<IFile> f = createIFile(path);
        if(f)
        {
            CDateTime modtime;
            f->getTime(NULL, &modtime, NULL);
            m_modtime = modtime.getSimple();
            m_size = f->size();
        }
        else
        {
            m_size = 0;
            m_modtime = 0;
        }
    }

    CXslIncludeSignature(const char* path, MemoryBuffer& buf)
    {
        if(!path || !*path)
            throw MakeStringException(-1, "CXslIncludeSignature : path can't be emtpy");

        m_type = IO_TYPE_BUFFER;
        m_size = buf.length();
        m_crc = ~crc32(buf.toByteArray(), m_size, ~0);
    }

    bool operator==(CXslIncludeSignature& incl)
    {
        if(m_type != incl.m_type)
            return false;
        if(m_type == IO_TYPE_FILE)
            return (strcmp(m_path.str(), incl.m_path.str()) == 0) && (m_size == incl.m_size) && (m_modtime == incl.m_modtime);
        else
            return (m_size == incl.m_size) && (m_crc == incl.m_crc);
    }

    const char* getPath()
    {
        return m_path.str();
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
            CXslIncludeSignature* inc1p = &m_signatures.item(x);
            if(!inc1p)
                return true;
            CXslIncludeSignature inc2(inc1p->getPath());
            if(!(*inc1p == inc2))
                return true;
        }
        return false;
    }
};

class CXslEntry : public CInterface, implements IInterface
{
private:
    Owned<IXslBuffer> m_buffer;
    StringBuffer  m_fname;
    int m_size;
    unsigned long m_timestamp;
    unsigned long m_crc; 
    unsigned long m_modtime;
    IO_Type m_type;
    StringArray*  m_includepaths;
    Owned<CXslIncludeCompare> m_includecomp;

public:
    IMPLEMENT_IINTERFACE;

    CXslEntry(IXslBuffer* buffer)
    {
        m_size = 0;
        m_modtime = 0;
        m_crc = 0;
        m_timestamp = 0;
        setBuffer(buffer);
    }

    void setBuffer(IXslBuffer* buffer)
    {
        if(buffer)
        {
            time_t t;
            time(&t);
            m_timestamp = t;
        
            m_includepaths = &buffer->getIncludes();
            m_buffer.set(buffer);
            m_type = buffer->getType();
            if(m_type == IO_TYPE_FILE)
            {
                Owned<IFile> f = createIFile(buffer->getFileName());
                if(f)
                {
                    m_fname.clear().append(buffer->getFileName());
                    CDateTime modtime;
                    f->getTime(NULL, &modtime, NULL);
                    m_modtime = modtime.getSimple();
                    m_size = f->size();
                }
            }
            else 
            {
                m_size = buffer->getLen();
                m_crc = ~crc32(buffer->getBuf(), m_size, ~0);
            }
        }
        else
            throw MakeStringException(-1, "can't create CXslEntry with a NULL input");
    }

    IXslBuffer* getBuffer()
    {
        return m_buffer.get();
    }

    bool match(CXslEntry* entry)
    {
        if(!entry)
            return false;

        if(m_type != entry->m_type)
            return false;
        if(m_type == IO_TYPE_FILE)
            return (m_fname.length() > 0) && (entry->m_fname.length() > 0) && (strcmp(m_fname.str(), entry->m_fname.str()) == 0);
        else
            return (m_size == entry->m_size) && (m_crc == entry->m_crc);
    }

    bool equal(CXslEntry* entry, bool matched)
    {
        if(!entry)
            return false;

        if(!matched && !match(entry))
            return false;

        if (m_type == IO_TYPE_FILE && !((m_modtime == entry->m_modtime) && (m_size == entry->m_size)))
            return false;

        if(m_includecomp.get() && m_includecomp->hasChanged())
            return false;

        return true;
    }

    unsigned long getKeyHash()
    {
        if(m_type ==  IO_TYPE_FILE)
        {
            unsigned long keyhash = 5381;
            for(int i = 0; i < m_fname.length(); i++)
            {
                int c = m_fname.charAt(i);
                keyhash = ((keyhash << 5) + keyhash) + c;
            }
            return keyhash;
        }
        else
            return (m_size + m_crc)*127;
    }

    unsigned long getTimestamp()
    {
        return m_timestamp;
    }

    void compile(bool recompile)
    {
        if(m_buffer.get())
        {
            m_buffer->compile(recompile);
            if(m_includepaths && m_includepaths->length() > 0)
            {
                m_includecomp.setown(new CXslIncludeCompare(*m_includepaths));
            }
        }
    }
};

#define XSLTCACHESIZE 256

class CXslCache : public CInterface, implements IXslCache
{
private:
    CXslEntry* m_cache[XSLTCACHESIZE];
    Mutex m_mutex;
    int m_cachetimeout;

public:
    IMPLEMENT_IINTERFACE;
    
    CXslCache()
    {
        m_cachetimeout = XSLT_DEFAULT_CACHETIMEOUT;

        for(int i = 0; i < XSLTCACHESIZE; i++)
            m_cache[i] = NULL;
    }

    virtual ~CXslCache()
    {
        for(int i = 0; i < XSLTCACHESIZE; i++)
        {
            if(m_cache[i] != NULL)
                m_cache[i]->Release();;
        }
    }

    IXslBuffer* getCompiledXsl(IXslBuffer* xslbuffer, bool recompile)
    {
        if(!xslbuffer)
            return NULL;
        
        synchronized block(m_mutex);
        Owned<CXslEntry> newentry = new CXslEntry(xslbuffer);

        int start = (newentry->getKeyHash()) % XSLTCACHESIZE;
        int ind = start;
        int oldest = start;
        do
        {
            CXslEntry* oneentry = m_cache[ind];
            if(!oneentry)
                break;

            time_t t;
            time(&t);
            if(m_cachetimeout != -1 && (t >= oneentry->getTimestamp() + m_cachetimeout))
            {
                oneentry->Release();
                m_cache[ind] = NULL;
                break;
            }

            if(oneentry->match(newentry.get()))
            {
                if(!oneentry->equal(newentry, true))
                {
                    oneentry->Release();
                    m_cache[ind] = NULL;
                    break;
                }

                if(recompile)
                {
                    oneentry->setBuffer(xslbuffer);
                    oneentry->compile(true);
                    return xslbuffer;
                }
                else
                    return oneentry->getBuffer();
            }

            if(oneentry->getTimestamp() < m_cache[oldest]->getTimestamp())
                oldest = ind;

            ind = (ind + 1) % XSLTCACHESIZE;
        }
        while(ind != start);

        newentry->compile(true);
        if(m_cache[ind] == NULL)
        {
            m_cache[ind] = newentry.getLink();
        }
        else
        {
            DBGLOG("XSLT cache is full, replacing oldest entry at index %d", oldest);
            if(m_cache[oldest] != NULL)
                m_cache[oldest]->Release();
            m_cache[oldest] = newentry.getLink();
        }

        return xslbuffer;
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
