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

#include "platform.h"
#include <math.h>
#include <stdio.h>
#include "jexcept.hpp"
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jlib.hpp"
#include "rtldistr.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/rtl/eclrtl/rtldistr.cpp $ $Id: rtldistr.cpp 64094 2011-04-19 08:13:22Z ghalliday $");

#define DISTRIBUTION_THRESHOLD 10000

//---------------------------------------------------------------------------

class CDistributionTable : public CInterface
{
protected:
    StringAttr fieldname;
public:
    IMPLEMENT_IINTERFACE;
    CDistributionTable(const char *_fieldname) : fieldname(_fieldname) {}
    virtual unsigned __int64 distinct() = 0;
    virtual bool exact() = 0;
    virtual void reportValues(StringBuffer &out) = 0;
    virtual void report(StringBuffer &out)
    {
        unsigned __int64 d = distinct();
        out.append("<Field name=\"").append(fieldname).append("\"");
        if (exact())
        {
            out.append(" distinct=\"").append(d).append("\">\n");
            reportValues(out);
            out.append("</Field>\n");
        }
        else
            out.append(" estimate=\"").append(d).append("\"/>\n");
    }
};

class CBoolDistributionTable : public CDistributionTable, implements IBoolDistributionTable
{
    unsigned __int64 counts[2];
public:
    IMPLEMENT_IINTERFACE;
    CBoolDistributionTable(const char *_fieldname) : CDistributionTable(_fieldname) 
    {
        counts[0] = counts[1] = 0;
    }
    virtual void report(StringBuffer &out)
    {
        CDistributionTable::report(out);
    }
    virtual void merge(MemoryBuffer &in)    
    { 
        unsigned __int64 c[2];
        in.read(sizeof(c), &c);
        counts[false] += c[false];
        counts[true] += c[true];
    }
    virtual void serialize(MemoryBuffer &out)   { out.append(sizeof(counts), &counts); }
    virtual void reportValues(StringBuffer &out)
    {
        if (counts[0])
            out.appendf(" <Value count=\"%"I64F"d\">false</Value>\n", counts[0]);
        if (counts[1])
            out.appendf(" <Value count=\"%"I64F"d\">true</Value>\n", counts[1]);
    }
    virtual void noteValue(bool val)
    {
        counts[val]++;
    }
    virtual unsigned __int64 distinct()
    {
        return (counts[0] != 0) + (counts[1] != 0);
    }
    virtual bool exact()
    {
        return true;
    }
};

class CByteDistributionTable : public CDistributionTable
{
    unsigned __int64 counts[256];
public:
    IMPLEMENT_IINTERFACE;
    CByteDistributionTable(const char *_fieldname) : CDistributionTable(_fieldname) 
    {
        memset(counts, 0, sizeof(counts));
    }
    virtual unsigned __int64 distinct()
    {
        unsigned __int64 ret = 0;
        for (unsigned i = 0; i < 256; i++)
            if (counts[i])
                ret++;
        return ret;
    }
    virtual bool exact()
    {
        return true;
    }
    virtual void merge(MemoryBuffer &in)    
    { 
        unsigned __int64 _counts[256];
        in.read(sizeof(_counts), &_counts); 
        for (unsigned i = 0; i < _elements_in(counts); i++)
            counts[i] += _counts[i];
    }
    virtual void serialize(MemoryBuffer &out)   { out.append(sizeof(counts), &counts); }
    virtual void reportValues(StringBuffer &out)
    {
        for (unsigned i = 0; i < 256; i++)
            if (counts[i])
            {
                out.appendf(" <Value count=\"%"I64F"d\">", counts[i]);
                reportValue(out, i);
                out.append("</Value>\n");
            }
    }
    virtual void reportValue(StringBuffer &out, unsigned val)
    {
        out.append(val);
    }
    void doNoteValue(unsigned int val)
    {
        counts[val]++;
    }
};

class CCharDistributionTable : public CByteDistributionTable, implements IStringDistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CCharDistributionTable(const char *_fieldname) : CByteDistributionTable(_fieldname) {}
    virtual void reportValue(StringBuffer &out, unsigned val)
    {
        unsigned char v = val;
        encodeXML((const char *) &v, out, ENCODE_WHITESPACE, 1);
    }
    virtual void report(StringBuffer &out)
    {
        CByteDistributionTable::report(out);
    }
    virtual void merge(MemoryBuffer &in)    { CByteDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CByteDistributionTable::serialize(out); }
    virtual void noteValue(unsigned len, const char *val)
    {
        assertex(len==1);
        doNoteValue((unsigned char) *val);
    }
};

class FixedMapper : public Mapping
{
public:
    FixedMapper(const void *k, int ksize);
    unsigned __int64 count;
};

FixedMapper::FixedMapper(const void *_key, int _ksize) : Mapping(_key, _ksize)
{
    count = 0;
}

class CFixedDistributionTable : public CDistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CFixedDistributionTable(const char *_fieldname, unsigned _ksize, unsigned _threshold) 
        : CDistributionTable(_fieldname), threshold(_threshold), table(_ksize, false), ksize(_ksize)
    {
        estimated = false;
        cardinality = 0;
    }
    virtual unsigned __int64 distinct() { return estimated ? cardinality : table.count(); }
    virtual bool exact() { return !estimated; }
    virtual void merge(MemoryBuffer &in)    
    {
        bool inEstimated;
        unsigned inNum, inCardinality;
        in.read(inCardinality).read(inEstimated).read(inNum);
        if (inEstimated) estimated = true;
        cardinality += inCardinality;
        for (unsigned idx=0; idx < inNum; idx++)
        {
            const void * key;
            unsigned __int64 count;
            key = in.readDirect(ksize);
            in.read(count);
            FixedMapper * mapped = queryLookup(key);
            if (mapped)
                mapped->count += count;
        }
    }
    virtual void serialize(MemoryBuffer &out)   
    { 
        out.append(cardinality);
        out.append(estimated);
        out.append(table.count());
        HashIterator iter(table);
        ForEach(iter)
        {
            FixedMapper & cur = (FixedMapper &) iter.get();
            out.append(ksize, cur.getKey()).append(cur.count);
        }
    }
    void addValue(const void *buf)
    {
        FixedMapper *mapped = queryLookup(buf);
        if (mapped)
            mapped->count++;
    }
    virtual void reportValue(StringBuffer &out, FixedMapper &val) = 0;
    virtual void reportValues(StringBuffer &out)
    {
        HashIterator iter(table);
        ForEach(iter)
        {
            FixedMapper & cur = (FixedMapper &) iter.get();
            out.appendf(" <Value count=\"%"I64F"d\">", cur.count);
            reportValue(out, cur);
            out.append("</Value>\n");
        }
    }

protected:
    FixedMapper * queryLookup(const void *buf)
    {
        if (estimated)
            return NULL;
        FixedMapper *mapped = (FixedMapper *) table.find(buf);
        if (!mapped)
        {
            mapped = new FixedMapper(buf, ksize);
            table.addOwn(*mapped);
            cardinality++;
            if (cardinality==threshold)
                estimated = true;
        }
        return mapped;
    }

protected:
    unsigned ksize;
    unsigned cardinality;
    unsigned threshold;
    bool estimated;
    KeptHashTable table;
};

class CIntDistributionTable : public CFixedDistributionTable, implements IIntDistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CIntDistributionTable(const char *_fieldname, unsigned threshold) : CFixedDistributionTable(_fieldname, sizeof(int), threshold)
    {
    }
    virtual void merge(MemoryBuffer &in)    { CFixedDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CFixedDistributionTable::serialize(out); }
    virtual void reportValue(StringBuffer &out, FixedMapper &val)
    {
        out.append(*(int *)val.getKey());
    }
    virtual void report(StringBuffer &out)
    {
        CFixedDistributionTable::report(out);
    }
    virtual void noteValue(int val)
    {
        addValue(&val);
    }
};

class CUIntDistributionTable : public CFixedDistributionTable, implements IUIntDistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CUIntDistributionTable(const char *_fieldname, unsigned threshold) : CFixedDistributionTable(_fieldname, sizeof(unsigned int), threshold)
    {
    }
    virtual void merge(MemoryBuffer &in)    { CFixedDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CFixedDistributionTable::serialize(out); }
    virtual void reportValue(StringBuffer &out, FixedMapper &val)
    {
        out.append(*(unsigned int *)val.getKey());
    }
    virtual void report(StringBuffer &out)
    {
        CFixedDistributionTable::report(out);
    }
    virtual void noteValue(unsigned int val)
    {
        addValue(&val);
    }
};

class CInt64DistributionTable : public CFixedDistributionTable, implements IInt64DistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CInt64DistributionTable(const char *_fieldname, unsigned threshold) : CFixedDistributionTable(_fieldname, sizeof(__int64), threshold)
    {
    }
    virtual void merge(MemoryBuffer &in)    { CFixedDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CFixedDistributionTable::serialize(out); }
    virtual void reportValue(StringBuffer &out, FixedMapper &val)
    {
        out.append(*(__int64 *)val.getKey());
    }
    virtual void report(StringBuffer &out)
    {
        CFixedDistributionTable::report(out);
    }
    virtual void noteValue(__int64 val)
    {
        addValue(&val);
    }
};

class CUInt64DistributionTable : public CFixedDistributionTable, implements IUInt64DistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CUInt64DistributionTable(const char *_fieldname, unsigned threshold) : CFixedDistributionTable(_fieldname, sizeof(unsigned __int64), threshold)
    {
    }
    virtual void merge(MemoryBuffer &in)    { CFixedDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CFixedDistributionTable::serialize(out); }
    virtual void reportValue(StringBuffer &out, FixedMapper &val)
    {
        out.append(*(unsigned __int64 *)val.getKey());
    }
    virtual void report(StringBuffer &out)
    {
        CFixedDistributionTable::report(out);
    }
    virtual void noteValue(unsigned __int64 val)
    {
        addValue(&val);
    }
};

class CRealDistributionTable : public CFixedDistributionTable, implements IRealDistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CRealDistributionTable(const char *_fieldname, unsigned threshold) : CFixedDistributionTable(_fieldname, sizeof(double), threshold)
    {
    }
    virtual void merge(MemoryBuffer &in)    { CFixedDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CFixedDistributionTable::serialize(out); }
    virtual void reportValue(StringBuffer &out, FixedMapper &val)
    {
        out.append(*(double *)val.getKey());
    }
    virtual void report(StringBuffer &out)
    {
        CFixedDistributionTable::report(out);
    }
    virtual void noteValue(double val)
    {
        addValue(&val);
    }
};

class CStringDistributionTable : public CFixedDistributionTable, implements IStringDistributionTable
{
public:
    IMPLEMENT_IINTERFACE;
    CStringDistributionTable(const char *_fieldname, unsigned _ksize, unsigned threshold) : CFixedDistributionTable(_fieldname, _ksize, threshold)
    {
    }
    virtual void merge(MemoryBuffer &in)    { CFixedDistributionTable::merge(in); }
    virtual void serialize(MemoryBuffer &out)   { CFixedDistributionTable::serialize(out); }
    virtual void reportValue(StringBuffer &out, FixedMapper &val)
    {
        encodeXML((const char *) val.getKey(), out, ENCODE_WHITESPACE, ksize);
    }
    virtual void report(StringBuffer &out)
    {
        CFixedDistributionTable::report(out);
    }
    virtual void noteValue(unsigned len, const char *val)
    {
        assertex(len==ksize);
        addValue(val);
    }
};

//--------------------------------------------------------------------------------------

ECLRTL_API IStringDistributionTable *createIStringDistributionTable(const char *name, unsigned size)
{
    switch (size)
    {
    case 0:
//  case UNKNOWN_LENGTH:
        assertex(false); // TBD
    case 1:
        return new CCharDistributionTable(name);
    default:
        return new CStringDistributionTable(name, size, DISTRIBUTION_THRESHOLD);
    }
}

ECLRTL_API IRealDistributionTable *createIRealDistributionTable(const char *name, unsigned size)
{
    return new CRealDistributionTable(name, DISTRIBUTION_THRESHOLD);
}

ECLRTL_API IBoolDistributionTable *createIBoolDistributionTable(const char *name, unsigned size)
{
    return new CBoolDistributionTable(name);
}

ECLRTL_API IIntDistributionTable *createIIntDistributionTable(const char *name, unsigned size)
{
    // MORE - could optimize size 1
    return new CIntDistributionTable(name, DISTRIBUTION_THRESHOLD);
}

ECLRTL_API IInt64DistributionTable *createIInt64DistributionTable(const char *name, unsigned size)
{
    return new CInt64DistributionTable(name, DISTRIBUTION_THRESHOLD);
}

ECLRTL_API IUIntDistributionTable *createIUIntDistributionTable(const char *name, unsigned size)
{
    return new CUIntDistributionTable(name, DISTRIBUTION_THRESHOLD);
}

ECLRTL_API IUInt64DistributionTable *createIUInt64DistributionTable(const char *name, unsigned size)
{
    return new CUInt64DistributionTable(name, DISTRIBUTION_THRESHOLD);
}

