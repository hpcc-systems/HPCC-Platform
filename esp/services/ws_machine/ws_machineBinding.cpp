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

// WsMachineBinding.cpp: implementation of the CWsMachineSoapBindingEx class.
//
//////////////////////////////////////////////////////////////////////
#if defined(_WIN32) && defined(_DEBUG)
//disable harmless warning about symbol names too long in debug build
#pragma warning(disable:4786)
#endif
#include "ws_machineBinding.hpp"


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
         m_processTypes.sortAscii();
      }
   }
}
