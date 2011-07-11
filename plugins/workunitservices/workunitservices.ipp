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

inline void varAppend(MemoryBuffer &mb,unsigned w,IPropertyTree &pt,const char *prop)
{
    StringBuffer s;
    pt.getProp(prop,s);
    size32_t sz = s.length();
    if (sz>w)
        sz = w;
    mb.append(sz).append(sz,s.str());
}

inline bool serializeWUSrow(IPropertyTree &pt,MemoryBuffer &mb, bool isonline)
{
    fixedAppend(mb,24,pt.queryName());
    varAppend(mb,64,pt,"@submitID");
    varAppend(mb,64,pt,"@clusterName");
    varAppend(mb,64,pt,"RoxieQueryInfo/@roxieClusterName");
    varAppend(mb,256,pt,"@jobName");
    fixedAppend(mb,10,pt,"@state");
    fixedAppend(mb,7,pt,"@priorityClass");
    const char *mod = "TimeStamps/TimeStamp[@application=\"workunit\"]/Modified";
    const char *crt = "TimeStamps/TimeStamp[@application=\"workunit\"]/Created";
    fixedAppend(mb,20,pt,crt);
    if (pt.hasProp(mod))
        fixedAppend(mb,20,pt,mod);
    else
        fixedAppend(mb,20,pt,crt);
    byte online = isonline?1:0;
    mb.append(online);
    byte prot = pt.getPropBool("@protected")?1:0;
    mb.append(prot);
    if (mb.length()>WORKUNIT_SERVICES_BUFFER_MAX) {
        mb.clear().append(WUS_STATUS_OVERFLOWED);
        return false;
    }
    return true;
}

#endif

