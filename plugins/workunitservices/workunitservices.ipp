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

#ifndef _WORKUNITSERVICES_IPP_INCL
#define _WORKUNITSERVICES_IPP_INCL

// pure inlines - used by workunitservices and sasha to serialize rows


#define WORKUNIT_SERVICES_BUFFER_MAX (0x100000-1024) 
#define WUS_STATUS_OVERFLOWED   ((byte)1)


inline void fixedAppend(MemoryBuffer &mb,unsigned w,const char *s, size32_t l=0)
{
    if (!s)
        l = 0;
    else if (l==0)
        l = strlen(s);
    if (l>w)
        l = w;
    mb.append(l,s);
    while (l++<w)
        mb.append(' ');
}

inline void fixedAppend(MemoryBuffer &mb,unsigned w,IPropertyTree &pt,const char *prop)
{
    StringBuffer s;
    pt.getProp(prop,s);
    fixedAppend(mb,w,s.str(),s.length());
}

inline void varAppend(MemoryBuffer &mb,size32_t sz, const char * s)
{
    mb.append(sz).append(sz,s);
}

inline void varAppend(MemoryBuffer &mb,const char * s)
{
    varAppend(mb, strlen(s), s);
}

inline void varAppendMax(MemoryBuffer &mb,unsigned w,IPropertyTree &pt,const char *prop)
{
    StringBuffer s;
    pt.getProp(prop,s);
    size32_t sz = s.length();
    if (sz>w)
        sz = w;
    mb.append(sz).append(sz,s.str());
}

inline void varAppendMax(MemoryBuffer &mb,unsigned w,const char *str, size32_t l=0)
{
    if (!str)
        l = 0;
    else if (l==0)
        l = strlen(str);
    if (l>w)
        l = w;
    mb.append(l).append(l, str);
}

// This is use by sasha - it's a real mess

inline void convertTimestampToStr(unsigned __int64 timestamp, StringBuffer& timeStr, bool formatTZ)
{
    formatStatistic(timeStr, timestamp, SMeasureTimestampNs);
    if (formatTZ)
    {
        timeStr.setCharAt(19, 'Z'); //Match with old timestamp
        timeStr.setLength(20); //Match with old timestamp
    }
}

inline const char* readCreateTime(IPropertyTree& pt, StringBuffer& time, bool formatTZ)
{
    time.clear();
    unsigned __int64 value = pt.getPropInt64("Statistics/Statistic[@s='global'][@kind='WhenCreated']/@value", 0);
    if (value > 0)
        convertTimestampToStr(value, time, formatTZ);
    return time.str();
}

inline const char* readModifyTime(IPropertyTree& pt, StringBuffer& time, bool formatTZ)
{
    time.clear();
    unsigned __int64 value = 0;
    Owned<IPropertyTreeIterator> stats = pt.getElements("Statistics/Statistic[@s='global'][@kind='WhenWorkunitModified']");
    ForEach(*stats)
    {
        IPropertyTree& stat = stats->query();
        unsigned __int64 val = stat.getPropInt64("@value", 0);
        if (val > value)
            value = val;
    }
    if (value > 0)
        convertTimestampToStr(value, time, formatTZ);
    return time.str();
}

inline bool serializeWUSrow(IPropertyTree &pt, MemoryBuffer &mb, size32_t maxBufferLength, bool isonline)
{
    mb.setEndian(__LITTLE_ENDIAN);
    fixedAppend(mb,24,pt.queryName());
    varAppendMax(mb,64,pt,"@submitID");
    varAppendMax(mb,64,pt,"@clusterName");
    varAppendMax(mb,64,pt,"RoxieQueryInfo/@roxieClusterName"); // No longer likely to be present but retain for compatibility (for now - this needs rewriting to not hit dali directly anyway!)
    varAppendMax(mb,256,pt,"@jobName");
    fixedAppend(mb,10,pt,"@state");
    fixedAppend(mb,7,pt,"@priorityClass");
    short int prioritylevel = calcPriorityValue(&pt);
    mb.appendEndian(sizeof(prioritylevel), &prioritylevel);

    StringBuffer crtTime, modTime;
    readModifyTime(pt, modTime, true);
    readCreateTime(pt, crtTime, true);
    if (crtTime.length())
    {
        fixedAppend(mb, 20, crtTime.str(), crtTime.length());
        if (modTime.length())
            fixedAppend(mb, 20, modTime.str(), modTime.length());
        else
            fixedAppend(mb, 20, crtTime.str(), crtTime.length());
    }
    else
    {
        const char *mod = "TimeStamps/TimeStamp[@application=\"workunit\"]/Modified";
        const char *crt = "TimeStamps/TimeStamp[@application=\"workunit\"]/Created";
        fixedAppend(mb,20,pt,crt);
        if (pt.hasProp(mod))
            fixedAppend(mb,20,pt,mod);
        else
            fixedAppend(mb,20,pt,crt);
    }
    byte online = isonline?1:0;
    mb.append(online);
    byte prot = pt.getPropBool("@protected")?1:0;
    mb.append(prot);
    if (mb.length() > maxBufferLength)
    {
        mb.clear().append(WUS_STATUS_OVERFLOWED);
        return false;
    }
    return true;
}

#endif

