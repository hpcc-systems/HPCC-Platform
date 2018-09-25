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

#include "ComponentBase.hpp"
#include "deployutils.hpp"
#include "configenvhelper.hpp"
#include "buildset.hpp"

namespace ech
{

ComponentBase::ComponentBase(const char* name, EnvHelper * envHelper)
{
  this->m_name.clear().appendf("%s",name);
  assert(envHelper);
  this->m_envHelper = envHelper;
}

const char* ComponentBase::getAttributeFromParams(IPropertyTree *attrs, const char* attrName, const char* defaultValue)
{

   StringBuffer xpath;
   xpath.clear().appendf("Attribute[@name='%s']", attrName);

   //Owned<IPropertyTree> attr = attrs->queryPropTree(xpath.str());
   IPropertyTree *attr = attrs->queryPropTree(xpath.str());
   if (attr)
      return  attr->queryProp("@value");

   return defaultValue;
}

bool ComponentBase::validateAttribute(const char* xpath, const char* name, const char* value)
{
   return true;
}

bool ComponentBase::validateAttributes(const char* xpath, IPropertyTree* pAttrs)
{
   return true;
}

IPropertyTree * ComponentBase::createNode(IPropertyTree *pAttrs)
{

   IPropertyTree *pNode = createPTree();
   return updateNode(pNode, pAttrs);
}

IPropertyTree * ComponentBase::updateNode(IPropertyTree* pNode, IPropertyTree *pAttrs, StringArray* excludeList)
{
   assert(pNode);

   Owned<IPropertyTreeIterator> attrsIter = pAttrs->getElements("Attribute");
   ForEach(*attrsIter)
   {
      IPropertyTree* pAttrTree = &attrsIter->query();
      const char* attrName = pAttrTree->queryProp("@name");

      if (excludeList && excludeList->find(attrName) != NotFound)
         continue;
      const char* attrValue = pAttrTree->queryProp("@value");
      const char* attrOldValue = pAttrTree->queryProp("@oldValue");

      StringBuffer sb;
      sb.clear().appendf("@%s",attrName);

      const char* curValue = pNode->queryProp(sb.str());
      if (curValue && attrOldValue && stricmp(curValue, attrOldValue))
      {
         continue;
      }
      else
      {
         pNode->setProp(sb.str(), attrValue);
      }
   }
   return pNode;
}

IPropertyTree * ComponentBase::removeAttributes(IPropertyTree* pNode, IPropertyTree *pAttrs, StringArray* excludeList)
{
   assert(pNode);
   Owned<IPropertyTreeIterator> attrsIter = pAttrs->getElements("Attribute");
   ForEach(*attrsIter)
   {
      StringBuffer sb;
      IPropertyTree* pAttrTree = &attrsIter->query();
      const char* attrName = pAttrTree->queryProp("@name");
      if (excludeList && excludeList->contains(attrName))
         continue;
      sb.clear().appendf("@%s",attrName);
      const char* currentValue = pNode->queryProp(sb.str());
      if (!currentValue) return pNode;

      const char* targetValue  = pAttrTree->queryProp(sb.str());

      if (targetValue && stricmp(currentValue, targetValue))
         continue;

      pNode->removeProp(sb.str());

   }

   return pNode;

}

bool ComponentBase::isNodeMatch(IPropertyTree *pNode, IPropertyTree *pAttrs)
{
   Owned<IPropertyTreeIterator> attrsIter = pAttrs->getElements("Attribute");
   bool match =  true;
   StringBuffer xpath;
   ForEach(*attrsIter)
   {
      IPropertyTree* pAttr = &attrsIter->query();
      xpath.clear().appendf("@%s", pAttr->queryProp("@name"));
      if (!pNode->hasProp(xpath.str()))
      {
         match = false;
         break;
      }

      const char * value = pAttr->queryProp("@value");
      if (!value || stricmp(value, pNode->queryProp(xpath.str())))
      {
         match = false;
         break;
      }
   }
   return match;
}

IPropertyTree * ComponentBase::getNodeByAttributes(IPropertyTree* parent, const char* xpath,  IPropertyTree *pAttrs)
{
   if (!pAttrs)
   {
      return parent->queryPropTree(xpath);
   }

   Owned<IPropertyTreeIterator> nodesIter = parent->getElements(xpath);
   ForEach(*nodesIter)
   {
      IPropertyTree* pNode = &nodesIter->query();
      if (isNodeMatch(pNode, pAttrs))
         return pNode;
   }

   return NULL;
}

IPropertyTree* ComponentBase::getCompTree(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char* category = m_envHelper->getXMLTagName(params->queryProp("@category"));
   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));
   const char* key = params->queryProp("@key");

   IPropertyTree * categoryTree =  envTree->queryPropTree(category);

   IPropertyTree * compTree = NULL;
   if (compName)
   {
      StringBuffer xpath;
      if (key)
         xpath.clear().appendf("%s[@name=\"%s\"]",compName, key);
      else
         xpath.clear().appendf("%s[1]",compName);
      IPropertyTree * compTree = categoryTree->queryPropTree(xpath.str());
   }

   return compTree;
}

void ComponentBase::modify(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   IPropertyTree * categoryTree =  envTree->queryPropTree(
      m_envHelper->getXMLTagName(params->queryProp("@category")));

   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));
   const char* key = params->queryProp("@key");
   StringBuffer xpath;
   xpath.clear().appendf("%s",compName);
   if (key)
      xpath.appendf("[@name=\"%s\"]", key);

   const char* selector =  params->queryProp("@selector");
   StringArray excludeList;
   if (selector && (String(selector).toLowerCase())->startsWith("instance"))
   {
      // instance ip will be handled differently by SWProcess
      excludeList.append("ip");
      excludeList.append("ipfile");
   }

   StringBuffer selectorXPath;
   resolveSelector(selector, params->queryProp("@selector-key"), selectorXPath);

   synchronized block(mutex);
   Owned<IPropertyTreeIterator> compIter = categoryTree->getElements(xpath.str());

   IPropertyTree * pAttrs = params->queryPropTree("Attributes");
   if (pAttrs)
   {
      ForEach(*compIter)
      {
         IPropertyTree* compTree = &compIter->query();
         if (!selector)
         {
             updateNode(compTree, pAttrs, &excludeList);
         }
         else
         {
            Owned<IPropertyTreeIterator> selectorIter = compTree->getElements(selectorXPath.str());
            ForEach(*selectorIter)
            {
               IPropertyTree * pNode  = &selectorIter->query();
               updateNode(pNode, pAttrs, &excludeList);
            }
         }
      }
   }
}

void ComponentBase::remove(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   IPropertyTree * categoryTree =  envTree->queryPropTree(
      m_envHelper->getXMLTagName(params->queryProp("@category")));

   const char* target = params->queryProp("@target");
   const char* selector = params->queryProp("@selector");

   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));
   const char* key = params->queryProp("@key");
   StringBuffer xpath;
   xpath.clear().appendf("%s",compName);
   if (key)
      xpath.appendf("[@name=\"%s\"]", key);

   IPropertyTree * pAttrs =  params->queryPropTree("Attributes");
   Owned<IPropertyTreeIterator> compIter = categoryTree->getElements(xpath.str());
   synchronized block(mutex);
   ForEach(*compIter)
   {
      IPropertyTree* compTree = &compIter->query();

      // remove attributes
      if (target && !stricmp(target, "attribute"))
      {
         IPropertyTree * pNode = (selector)? compTree->queryPropTree(selector): compTree;
         removeAttributes (pNode, params->queryPropTree("Attributes"));
         continue;
      }

      // remove selector node
      if (selector && (!target || stricmp(target, "child")))
      {
         // get node by attributes
         IPropertyTree * pNode = getNodeByAttributes(compTree, selector, pAttrs);
         if (pNode)
         {
            compTree->removeTree(pNode);
         }
         continue;
      }

      // For example:  EnvSettings entries
      if (selector && !stricmp(target, "child"))
      {
         IPropertyTree * pNode = (compTree)? compTree: categoryTree;
         IPropertyTree * children = params->queryPropTree("Children");
         if (!children)  continue;
         Owned<IPropertyTreeIterator> childIter = children->getElements("Child");
         ForEach(*childIter)
         {
            IPropertyTree* child = &childIter->query();
            if (child) pNode->removeTree(child);
         }
         continue;
      }

      // remove component
      if (!pAttrs || isNodeMatch(compTree, pAttrs))
      {
         categoryTree->removeTree(compTree);
      }
   }
}

IPropertyTree * ComponentBase::createChildrenNodes(IPropertyTree *parent, IPropertyTree *children)
{
   Owned<IPropertyTreeIterator> iter =  children->getElements("Child");
   StringBuffer xpath;
   ForEach (*iter)
   {
      IPropertyTree *child = &iter->query();
      const char* childName = m_envHelper->getXMLTagName(child->queryProp("@name"));
      IPropertyTree *pAttrs = child->queryPropTree("Attributes");
      xpath.clear().appendf("%s[1]", childName);
      IPropertyTree* pChild = parent->queryPropTree(xpath.str());
      if (pChild)
      {
         updateNode(pChild, pAttrs);
      }
      else
      {
         parent->addPropTree(childName, createNode(pAttrs));
      }
   }

   return parent;
}

IPropertyTree * ComponentBase::cloneComponent(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char* categoryName = m_envHelper->getXMLTagName(params->queryProp("@category"));
   const char* compName = m_envHelper->getXMLTagName(params->queryProp("@component"));

   const char* key = params->queryProp("@key");
   if (!key)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss %s target name to clone", compName);

   const char* cloneKey = params->queryProp("@clone");
   if (!cloneKey)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Miss %s source name to clone", compName);

   IPropertyTree *category = envTree->queryPropTree(categoryName);
   StringBuffer xpath;
   xpath.clear().appendf("%s[%s=\"%s\"]", compName, XML_ATTR_NAME, key);
   IPropertyTree *targetNode = category->queryPropTree(xpath.str());
   if (targetNode) return targetNode;

   xpath.clear().appendf("%s[%s=\"%s\"]", compName, XML_ATTR_NAME, cloneKey);
   IPropertyTree *sourceNode = category->queryPropTree(xpath.str());
   if (!sourceNode)
      throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Can't find source node %s to clone", xpath.str());

   StringBuffer xml;
   toXML(sourceNode, xml);
   targetNode = createPTreeFromXMLString(xml.str());
   targetNode->setProp(XML_ATTR_NAME, key);

   IPropertyTree * attrs = params->queryPropTree("Attributes");
   if (attrs)
      updateNode(targetNode, attrs);

   category->addPropTree(compName, targetNode) ;

   return targetNode;

}

void ComponentBase::resolveSelector(const char* selector, const char* key, StringBuffer& out)
{
   if (!key || !(*key))
      out.clear().append(selector);

   String lwSelector(selector);
   if ((lwSelector.toLowerCase())->startsWith("instance"))
      out.clear().appendf("%s[@netAddress=\"%s\"]", selector, key);
   else if (key)
      out.clear().appendf("%s[@name=\"%s\"]", selector, key);
   else
      out.clear().appendf("%s", selector);

}


}
