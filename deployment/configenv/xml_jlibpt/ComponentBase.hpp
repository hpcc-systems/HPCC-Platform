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
#ifndef _COMPONENTBASE_HPP_
#define _COMPONENTBASE_HPP_

#include "EnvHelper.hpp"

namespace ech
{


class ComponentBase : public CInterface, implements IConfigComp
{
public:
   ComponentBase(const char* name, EnvHelper * envHelper);

   virtual ~ComponentBase(){};

   virtual void create(IPropertyTree *params) {};
   virtual unsigned add(IPropertyTree *params) { return 0; };
   virtual void modify(IPropertyTree *params);
   virtual void remove(IPropertyTree *params);

   IMPLEMENT_IINTERFACE;

   virtual IPropertyTree * cloneComponent(IPropertyTree *params);

   virtual void resolveSelector(const char* selector, const char* key, StringBuffer& out);

   const char* getAttributeFromParams(IPropertyTree *attrs, const char* attrName, const char* defaultValue);
   bool validateAttribute(const char* xpath, const char* name, const char* value);
   bool validateAttributes(const char* xPath, IPropertyTree *pAttrs);

   IPropertyTree * getCompTree(IPropertyTree *params);
   IPropertyTree * updateNode(IPropertyTree* pNode, IPropertyTree *pAttrs, StringArray *exludeList = NULL);
   IPropertyTree * createNode(IPropertyTree *pAttrs);
   // If allow multiple same child tag name it need another parameter. Currently only allow one.
   // If already exists it will only update attributes instead of creating a new node
   IPropertyTree * createChildrenNodes(IPropertyTree *parent, IPropertyTree *children);

   IPropertyTree * removeAttributes(IPropertyTree* pNode, IPropertyTree *pAttrs, StringArray *exludeList = NULL);
   IPropertyTree * getNodeByAttributes(IPropertyTree* parent, const char* xpath,  IPropertyTree *pAttrs);
   bool isNodeMatch(IPropertyTree *pNode, IPropertyTree *pAttrs);

protected:
   Mutex mutex;
   EnvHelper * m_envHelper;
   StringBuffer m_name;
};

}

#endif
