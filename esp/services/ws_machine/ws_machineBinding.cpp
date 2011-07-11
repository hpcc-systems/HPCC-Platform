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

// WsMachineBinding.cpp: implementation of the CWsMachineSoapBindingEx class.
//
//////////////////////////////////////////////////////////////////////
#if defined(_WIN32) && defined(_DEBUG)
//disable harmless warning about symbol names too long in debug build
#pragma warning(disable:4786)
#endif
#include "ws_machineBinding.hpp"


int SortFunction(const char** s1, const char** s2)
{
   return strcmp(*s1, *s2);
}

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

CWsMachineSoapBindingEx::CWsMachineSoapBindingEx(IPropertyTree* cfg, 
                                                 const char *bindname/*=NULL*/, 
                                                 const char *procname/*=NULL*/)
   :Cws_machineSoapBinding(cfg, bindname, procname)
{
    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]", procname);
   IPropertyTree* pEspProcess = cfg->queryPropTree(xpath.str());

   if (pEspProcess)
   {
       xpath.clear().appendf("EspBinding[@name=\"%s\"]/@service", bindname);
      const char* service = pEspProcess->queryProp(xpath.str());

      if (service)
      {
          xpath.clear().appendf("EspService[@name=\"%s\"]/MachineInfo/Software/*", service);
         Owned<IPropertyTreeIterator> it = pEspProcess->getElements(xpath.str());
         ForEach(*it)
         {
            m_processTypes.append(it->query().queryName());
         }
         m_processTypes.sort(SortFunction);
      }
   }
}
