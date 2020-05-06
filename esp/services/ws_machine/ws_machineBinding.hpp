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

// WsMachineBinding.h: interface for the CWsMachineSoapBindingEx class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_WSMACHINEBINDING_H__01817C00_79D1_4CAC_9890_A5F33437028B__INCLUDED_)
#define AFX_WSMACHINEBINDING_H__01817C00_79D1_4CAC_9890_A5F33437028B__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000

#include "ws_machine_esp.ipp"


class CWsMachineSoapBindingEx : public Cws_machineSoapBinding
{
public:
   CWsMachineSoapBindingEx(IPropertyTree* cfg, const char *bindname/*=NULL*/, 
                           const char *procname/*=NULL*/);

   virtual ~CWsMachineSoapBindingEx(){} 

    virtual int getMethodDescription(IEspContext &context, const char *serv, 
                                    const char *method, StringBuffer &page)
    {
        if (Utils::strcasecmp(method, "GetMachineInfo")==0)
        {
            page.append("Submit addresses of machines to retrieve their preflight information.");
        }
        return 0;
    }
    virtual int getMethodHelp(IEspContext &context, const char *serv, 
                             const char *method, StringBuffer &page)
    {
        if (Utils::strcasecmp(method, "GetMachineInfo")==0)
        {
            page.append("Enter security string (password) and one or more ");
         page.append("addresses of machines or address ranges.<br/>");
         page.append("Enter Push the \"Submit\" button.");
        }
        return 0;
    }

    virtual void getNavigationData(IEspContext &context, IPropertyTree & data)
    {
        if (queryComponentConfig().getPropBool("@api_only"))
        {
            CHttpSoapBinding::getNavigationData(context, data);
            return;
        }
    }

private:
   StringArray m_processTypes;
};
#endif // !defined(AFX_WSMACHINEBINDING_H__01817C00_79D1_4CAC_9890_A5F33437028B__INCLUDED_)


