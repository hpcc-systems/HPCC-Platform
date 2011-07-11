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

// WUXMLInfo.h: interface for the CWUXMLInfo class.
//
//////////////////////////////////////////////////////////////////////
#ifndef __CWUXMLInfo_HPP__
#define __CWUXMLInfo_HPP__

#ifdef SMCLIB_EXPORTS
    #define WUXMLINFO_API __declspec(dllexport)
#else
    #define WUXMLINFO_API __declspec(dllimport)
#endif

#include "jiface.hpp"
#include "workunit.hpp"
#include "jstring.hpp"
#include "jptree.hpp"
#include "ws_workunits.hpp"


class WUXMLINFO_API CWUXMLInfo : public CInterface  
{
    void formatDuration(StringBuffer &ret, unsigned ms);
public:
    IMPLEMENT_IINTERFACE;
    CWUXMLInfo();
    virtual ~CWUXMLInfo();
    bool buildXmlGraphList(IConstWorkUnit &wu, IPropertyTree& XMLStructure);
    bool buildXmlResultList(IConstWorkUnit &wu,IPropertyTree& XMLStructure);
    bool buildXmlActionList(IConstWorkUnit &wu, StringBuffer& statusStr);
    bool buildXmlWuidInfo(IConstWorkUnit &wu, IEspECLWorkunit& wuStructure,bool bDescription = false);
    bool buildXmlWuidInfo(IConstWorkUnit &wu, StringBuffer& wuStructure,bool bDescription = false);
    bool buildXmlWuidInfo(const char* wu, IEspECLWorkunit& wuStructure,bool bDescription = false);
    bool buildXmlExceptionList(IConstWorkUnit &wu,IPropertyTree& XMLStructure);
    bool buildXmlTimimgList(IConstWorkUnit &wu,IPropertyTree& XMLStructure);
    bool buildXmlLogList(IConstWorkUnit &wu,IPropertyTree& XMLStructure);
    void buildXmlActiveWuidStatus(const char* ClusterName, IEspECLWorkunit& wuStructure);
};

#endif // __CWUXMLInfo_HPP__
