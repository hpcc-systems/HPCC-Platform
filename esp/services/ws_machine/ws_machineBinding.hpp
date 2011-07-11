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
    }

private:
   StringArray m_processTypes;
};
#endif // !defined(AFX_WSMACHINEBINDING_H__01817C00_79D1_4CAC_9890_A5F33437028B__INCLUDED_)


