/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2018 HPCC Systems®.

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
#ifndef _SWPROCESS_HPP_
#define _SWPROCESS_HPP_

#include "EnvHelper.hpp"
#include "ComponentBase.hpp"
#include "SWComponentBase.hpp"

namespace ech
{

class SWProcess : public SWComponentBase
{
public:
   SWProcess(const char* name, EnvHelper * envHelper);
   virtual ~SWProcess() {}

   virtual void create(IPropertyTree *params);
   virtual unsigned add(IPropertyTree *params);
   virtual void modify(IPropertyTree *params);
   //virtual void remove(IPropertyTree *params);
   virtual IPropertyTree * addComponent(IPropertyTree *params);
   virtual IPropertyTree * cloneComponent(IPropertyTree *params);

   virtual void computerAdded(IPropertyTree *computerNode, const char* instanceXMLTagName=NULL);
   virtual void computerUpdated(IPropertyTree *computerNode, const char *oldName, const char *oldIp, const char* instanceXMLTagName=NULL);
   virtual void computerDeleted(const char *ipAddress, const char *computerName, const char* instanceXMLTagName=NULL);

   virtual void checkInstanceAttributes(IPropertyTree *instanceNode, IPropertyTree *parent);
   virtual void processNameChanged(const char* process, const char *newName, const char* oldName);

   virtual void resolveSelector(const char* selector, const char* key, StringBuffer& out);
   virtual void removeInstancesFromComponent(IPropertyTree *compNode);

   virtual void addInstances(IPropertyTree *parent, IPropertyTree *params);
   virtual void addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceTagXMLName);
   void modifyInstance(IPropertyTree *parent, IPropertyTree *params);
   virtual const char * getInstanceXMLTagName(const char* name) { return m_instanceElemName.str(); }
   IPropertyTree * addComputer(const char* ip);
   virtual IPropertyTree * findInstance(IPropertyTree *comp, IPropertyTree* computerNode);

   IConfigComp* getInstanceNetAddresses(StringArray& ipList, const char* clusterName=NULL);
   unsigned getInstanceCount(const char* clusterName=NULL);

   IPropertyTree * getPortDefinition();
   bool portIsRequired();
   int getDefaultPort();


protected:
   StringBuffer m_instanceElemName;
   StringBuffer m_ipAttribute;
   StringArray  m_notifyTopologyList;

   // only one instance should be active
   // if instances alraedy exist add/modify will be applied to the first instance
   StringArray  m_singleInstanceList;

};

}
#endif
