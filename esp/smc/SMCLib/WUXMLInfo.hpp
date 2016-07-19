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

// WUXMLInfo.h: interface for the CWUXMLInfo class.
//
//////////////////////////////////////////////////////////////////////
#ifndef __CWUXMLInfo_HPP__
#define __CWUXMLInfo_HPP__

#ifdef SMCLIB_EXPORTS
    #define WUXMLINFO_API DECL_EXPORT
#else
    #define WUXMLINFO_API DECL_IMPORT
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
//    bool buildXmlTimimgList(IConstWorkUnit &wu,IPropertyTree& XMLStructure);
    bool buildXmlLogList(IConstWorkUnit &wu,IPropertyTree& XMLStructure);
    void buildXmlActiveWuidStatus(const char* ClusterName, IEspECLWorkunit& wuStructure);
};

#endif // __CWUXMLInfo_HPP__
