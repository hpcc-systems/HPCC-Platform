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
#ifndef _SWTHORCLUSTER_HPP_
#define _SWTHORCLUSTER_HPP_

#include "EnvHelper.hpp"
#include "SWProcess.hpp"

namespace ech
{

class SWThorCluster : public SWProcess
{
public:
    SWThorCluster(const char* name, EnvHelper * envHelper);

    virtual void addInstances(IPropertyTree *parent, IPropertyTree *params);
    virtual void checkInstanceAttributes(IPropertyTree *instanceNode, IPropertyTree *parent);
    virtual const char* getInstanceXMLTagName(const char* name);

    IPropertyTree * addComponent(IPropertyTree *params);
    IPropertyTree * cloneComponent(IPropertyTree *params);
    void removeInstancesFromComponent(IPropertyTree *compNode);

    virtual void computerUpdated(IPropertyTree *computerNode, const char *oldName, const char *oldIp, const char* instanceXMLTagName=NULL);
    virtual void computerDeleted(const char *ipAddress, const char *computerName, const char* instanceXMLTagName=NULL);
    void updateComputerAttribute(const char *newName, const char *oldName);
    virtual void addInstance(IPropertyTree *computerNode, IPropertyTree *parent, IPropertyTree *attrs, const char* instanceTagXMLName);

private:
    StringBuffer m_masterInstanceElemName;

};

}
#endif
