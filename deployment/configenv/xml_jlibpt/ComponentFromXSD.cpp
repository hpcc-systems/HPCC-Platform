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
#include "deployutils.hpp"
#include "jliball.hpp"
//#include "computerpicker.hpp"
//#include "configenvhelper.hpp"
//#include "configengcallback.hpp"
//#include "xslprocessor.hpp"
//#include "jwrapper.hpp"
//#include "wizardInputs.hpp"
#include "build-config.h"
//#include "confighelper.hpp"
#include "ComponentFromXSD.hpp"
#include "EnvHelper.hpp"
#include "SWProcess.hpp"

namespace ech
{
#define TRACE_SCHEMA_NODE(msg, schemaNode)

//#define CONFIGMGR_JSPATH "./"
#define STANDARD_COMPFILESDIR INSTALL_DIR

#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml"


ComponentFromXSD::ComponentFromXSD(EnvHelper* peh):m_eh(peh), m_pCompTree(NULL),m_pDefTree(NULL),
  m_pSchemaRoot(NULL),m_numAttrs(0),m_allSubTypes(true),m_genOptional(true),m_wizFlag(true)
{
   m_pEnv.set(peh->getEnvTree());
}

ComponentFromXSD::~ComponentFromXSD()
{
  m_pEnv.clear();
  m_pSchemaRoot.clear();
}

void ComponentFromXSD::CreateAttributeFromSchema(IPropertyTree& attr,
     StringBuffer compName, const char* childElementName)
{
  StringBuffer attrname;
  StringBuffer strBuf;
  StringBuffer aName;
  StringBuffer value, tempPath, wizDefVal;
  attrname.append(attr.queryProp(XML_ATTR_NAME));

  const char *use = attr.queryProp("@use");
  if (!m_genOptional && use && *use && !strcmp(use, "optional"))
  {
    if (childElementName)
    {
      StringBuffer xpath;
      xpath.clear().append(childElementName);
      IPropertyTree* pChild = m_pCompTree->queryPropTree(xpath.str());

      if (!pChild)
        pChild = m_pCompTree->addPropTree(childElementName, createPTree());
    }

    return;
  }

  if (m_wizFlag)
  {
    if (attr.hasProp("./xs:annotation/xs:appinfo/autogenforwizard"))
    {
      value.clear().append(attr.queryProp("./xs:annotation/xs:appinfo/autogenforwizard"));
      if (!strcmp(value.str(),"1"))
      {
        getValueForTypeInXSD(attr, compName, wizDefVal);
      }
    }
    else
      return ;
  }

  if (childElementName)
    attrname.append(childElementName);

  const char *defaultValue = attr.queryProp("@default");
  StringBuffer sbdefaultValue;
  if (defaultValue)
  {
    sbdefaultValue.clear().append(defaultValue);
    sbdefaultValue.replaceString("\\", "\\\\");
  }

  if (wizDefVal.length() > 0)
  {
    sbdefaultValue.clear().append(wizDefVal);
  }

  if (m_pCompTree)
  {
    StringBuffer xpath;
    if (!childElementName)
    {
      xpath.clear().append("@").append(attrname);
      m_pCompTree->addProp(xpath, sbdefaultValue.str());
    }
    else
    {
      xpath.clear().append(childElementName);
      IPropertyTree* pChild = m_pCompTree->queryPropTree(xpath.str());

      if (!pChild)
        pChild = m_pCompTree->addPropTree(childElementName, createPTree());

      xpath.clear().append("@").append(attr.queryProp(XML_ATTR_NAME));
      pChild->addProp(xpath, sbdefaultValue.str());
    }
  }
}

void ComponentFromXSD::AddAttributeFromSchema(IPropertyTree& schemaNode,
       StringBuffer elemName, StringBuffer& compName, const char* childElementName)
{
  CreateAttributeFromSchema(schemaNode, compName,  childElementName);
}

void ComponentFromXSD::AddAttributesFromSchema(IPropertyTree* pSchema,
       StringBuffer& compName, const char* childElementName)
{
  if (pSchema)
  {
    //add attributes defined for this element
    Owned<IPropertyTreeIterator> attrIter = pSchema->getElements("xs:complexType/xs:attribute");
    ForEach(*attrIter)
    {
      AddAttributeFromSchema(attrIter->query(), "", compName, childElementName);
    }

    if (childElementName && !strcmp(childElementName, XML_TAG_INSTANCE))
    {
      const char* pszNameAttr = "<xs:attribute name='name' type='xs:string' use='optional'><xs:annotation><xs:appinfo><viewType>hidden</viewType></xs:appinfo></xs:annotation></xs:attribute>";
      Owned<IPropertyTree> pSchemaAttrNode = createPTreeFromXMLString(pszNameAttr);
      AddAttributeFromSchema(*pSchemaAttrNode, "", compName, childElementName);
    }

    // or if it's an attribute group, then try this variety...
    attrIter.setown(pSchema->getElements("xs:attribute"));
    ForEach(*attrIter)
    {
      AddAttributeFromSchema(attrIter->query(), "", compName, childElementName);
    }

    Owned<IPropertyTreeIterator> simple = pSchema->getElements("*");
    ForEach(*simple)
    {
      IPropertyTree &element = simple->query();
      const char* pszElementName = element.queryName();

      if (!strcmp(pszElementName, "xs:complexContent"))
        AddAttributesFromSchema(&element, compName, NULL);
    }
  }
}

void ComponentFromXSD::ProcessElementSchemaNode(IPropertyTree* pElement,
       IPropertyTree* pParentElement, StringBuffer& sbCompName)
{
  bool bOptSubType = false;
  if (pElement)
  {
    TRACE_SCHEMA_NODE("ProcessElementSchemaNode", pElement);
    const char* szParentElementName = pParentElement->queryProp(XML_ATTR_NAME);

    const char*  szElementName = pElement->queryProp(XML_ATTR_NAME);
    const char*  szCaption     = szElementName;
    //const char* tabName = pElement->queryProp("xs:annotation/xs:appinfo/title");
    //if (tabName)
    //  szCaption = tabName;

    IPropertyTree* pViewSchemaNode = pElement; //default for child view
    IPropertyTree* pInstanceNode = pParentElement;//default for child view

    bool bInstanceView = false;
    bool bViewChildNodes = true;

    if (pElement->hasProp("xs:annotation/xs:appinfo/viewType"))
    {
      const char* viewType = pElement->queryProp("xs:annotation/xs:appinfo/viewType");
      const char* viewChildNodes = pElement->queryProp("xs:annotation/xs:appinfo/viewChildNodes");
      bViewChildNodes = viewChildNodes && !stricmp(viewChildNodes, "true");

      bool needParent = true;

      //select first parent node that matches schema
      if (pInstanceNode && needParent)
      {
        Owned<IPropertyTreeIterator> it = pInstanceNode->getElements(szElementName);
        if (it->first() && it->isValid())
          pInstanceNode = &it->query();
      }

      if (!strcmp(viewType, "Instance") || !strcmp(viewType, "instance") ||
          !strcmp(viewType, "RoxiePorts") || !strcmp(viewType, "RoxieSlaves"))
        bOptSubType = true;
    }

    bool bHasElements = schemaNodeHasElements(pElement) != NULL;

    if (bViewChildNodes)
    {
      bool bHasAttribs = schemaNodeHasAttributes(pElement);
      bool bHasAttribGroups = schemaNodeHasAttributeGroups(pElement);
      bool bMaxOccursOnce = !pElement->hasProp(XML_ATTR_MAXOCCURS) || !strcmp(pElement->queryProp(XML_ATTR_MAXOCCURS), "1");
      bOptSubType = !bMaxOccursOnce;

      //figure out the type of child view to create
      if (bHasElements)
      {
        // MORE - this assumes there is only one nested element type and that it is repeated....
        StringBuffer sbElemName(szElementName);
        if (bHasAttribs) //has child elements and attributes
        {
          Owned<IPropertyTreeIterator> iter = pElement->getElements("*");
          ForEach(*iter)
          {
            IPropertyTree &subSchemaElement = iter->query();
            const char* szSubElementName = subSchemaElement.queryName();
            StringBuffer sbSubElemName(szSubElementName);
            TRACE_SCHEMA_NODE("ProcessSchemaElement", &subSchemaElement);
            if (m_allSubTypes || !bOptSubType)
              ProcessComplexTypeSchemaNode(&subSchemaElement, m_pSchemaRoot, sbElemName);
          }
        }
        else
        {
          //no attributes
          if (bMaxOccursOnce)
          {
            //has child elements but no attribs and only occurs once
            //so skip this element and create node list view for its children
            pViewSchemaNode = schemaNodeHasElements(pElement);
            if (pInstanceNode)
            {
              IPropertyTree* pNewInstanceNode = pInstanceNode->queryPropTree(szElementName);

              if (!pNewInstanceNode)
                pNewInstanceNode = pInstanceNode->addPropTree(szElementName, createPTree());

              pInstanceNode = pNewInstanceNode;
            }

            szElementName = pViewSchemaNode->queryProp(XML_ATTR_NAME);
          }
        }
      }
      else
      {
        //no child elements
        if (bHasAttribs)
        {
          if (!bMaxOccursOnce) //occurs multiple times
          {
            //select first parent node that matches schema
            if (pInstanceNode)
            {
              Owned<IPropertyTreeIterator> it = pInstanceNode->getElements(szParentElementName);
              if (it->first() && it->isValid())
                pInstanceNode = &it->query();
            }
          }
        }
        else
        {
          const char* type = pElement->queryProp("@type");

          if (type && !strcmp(type, "xs:string"))
          {
            if (m_pCompTree)
            {
              StringBuffer sb(sbCompName);
              if (!m_pCompTree->queryPropTree(sbCompName.str()))
                m_pCompTree->addPropTree(sbCompName.str(), createPTree());

              sb.append("/").append(szElementName);
              m_pCompTree->addPropTree(sb.str()/*szElementName*/, createPTree());
            }
          }
        }
      }
    }


    if (m_allSubTypes || !bOptSubType)
      AddAttributesFromSchema(pViewSchemaNode, sbCompName, szElementName);

    /*
    if (bOptSubType && m_viewChildNodes.get() && m_multiRowNodes.get())
    {
      if (bHasElements)
        m_viewChildNodes->addProp("Node", szElementName);
      else
        m_multiRowNodes->addProp("Node", szElementName);
    }
    */

    if (pInstanceNode)
    {
      //select first child node for which we are creating view
      Owned<IPropertyTreeIterator> it = pInstanceNode->getElements(pElement->queryProp(XML_ATTR_NAME));
      pInstanceNode = (it->first() && it->isValid()) ? &it->query() : NULL;
    }

  }
}


void ComponentFromXSD::ProcessComplexTypeSchemaNode(IPropertyTree* schemaNode,
       IPropertyTree* pParentElement, StringBuffer& sbCompName)
{
  if (schemaNode)
  {
    TRACE_SCHEMA_NODE("ProcessComplexTypeSchemaNode", schemaNode);
    const char* szParentElementName = pParentElement->queryProp(XML_ATTR_NAME);

    //now process the rest...
    Owned<IPropertyTreeIterator> iter = schemaNode->getElements(XSD_TAG_ATTRIBUTE_GROUP);
    ForEach(*iter)
    {
      IPropertyTree &schemaElement = iter->query();
      const char* name = schemaElement.queryProp("@ref");

      StringBuffer xPath;
      xPath.append("//xs:attributeGroup[@name='").append(name).append("']");
      Owned<IPropertyTreeIterator> iter2 = m_pSchemaRoot->getElements(xPath.str());
      ForEach(*iter2)
      {
        IPropertyTree &agDef = iter2->query();
        if (agDef.hasProp("xs:annotation/xs:appinfo/title"))
          name = agDef.queryProp("xs:annotation/xs:appinfo/title");

        AddAttributesFromSchema(&agDef, sbCompName, NULL);

        break; // should be exactly one!
              // MORE - this will not get scoping correct. Do I care?
      }
    }
    iter.setown(schemaNode->getElements("*"));
    ForEach(*iter)
    {
      IPropertyTree &schemaElement = iter->query();
      const char* szSchemaElementName = schemaElement.queryName();

      if (!strcmp(szSchemaElementName, XSD_TAG_SEQUENCE) || !strcmp(szSchemaElementName, XSD_TAG_CHOICE))
      {
        Owned<IPropertyTreeIterator> iter2 = schemaElement.getElements(XSD_TAG_ELEMENT);
        ForEach(*iter2)
        {
          IPropertyTree* pElement = &iter2->query();
          ProcessElementSchemaNode(pElement, pParentElement, sbCompName);
        }
      }
    }
  }
}

bool ComponentFromXSD::generateHeaders()
{
  StringBuffer sbPropName;

  if (!m_pSchemaRoot)
    throw MakeStringException(-1, "Missing schema property tree");

  IPropertyTree *schemaNode = m_pSchemaRoot->queryPropTree("xs:element");

  if (m_compName.length() == 0)
    m_compName.append(schemaNode->queryProp(XML_ATTR_NAME));

  if (!strcmp(m_compName.str(), "Eclserver"))
    m_compName.clear().append(XML_TAG_ECLSERVERPROCESS);

  Owned<IPropertyTreeIterator> iter = schemaNode->getElements("*");
  ForEach(*iter)
  {
    IPropertyTree &schemaElement = iter->query();
    const char* szElementName = schemaElement.queryName();
    TRACE_SCHEMA_NODE("ProcessSchemaElement", &schemaElement);

    //if node is xs:complexType and xs:complexContent then process children
    if (!strcmp(szElementName, XSD_TAG_COMPLEX_TYPE) ||
        !strcmp(szElementName, XSD_TAG_COMPLEX_CONTENT))
    {
      //if this schema node has any attributes then add an attribute tab
      //
      bool bHasAttribs = schemaNodeHasAttributes(&schemaElement);
      if (bHasAttribs)
      {
        AddAttributesFromSchema(schemaNode, m_compName, NULL);
      }
    }

    ProcessComplexTypeSchemaNode(&schemaElement, m_pSchemaRoot, m_compName);
  }

  return true;
}

void ComponentFromXSD::setCompTree(const char* buildSetName, IPropertyTree* pTree, IPropertyTree* schemaTree, bool allSubTypes)
{
  m_buildSetName.clear().append(buildSetName);
  m_pCompTree = pTree;
  m_allSubTypes = allSubTypes;
  m_pSchemaRoot.set(schemaTree);
}

void ComponentFromXSD::getValueForTypeInXSD(IPropertyTree& attr,
       StringBuffer compName, StringBuffer& wizDefVal)
{
  StringBuffer tempPath;
  const char* type = attr.queryProp("@type");
  const char* name = attr.queryProp("@name");

  //first check for all the tags autogen then proceed with type checking.
  if (attr.hasProp("./xs:annotation/xs:appinfo/autogendefaultvalue"))
  {
    tempPath.clear().append("./xs:annotation/xs:appinfo/autogendefaultvalue");

    if (!strcmp(attr.queryProp(tempPath.str()), "$defaultenvfile"))
    {
      const IProperties* pParams =  m_eh->getEnvConfigOptions().getProperties();
      wizDefVal.clear().append(pParams->queryProp("configs")).append("/environment.xml");
    }
    else if (!strcmp(attr.queryProp(tempPath.str()), "$componentfilesdir"))
    {
      tempPath.clear().append("EnvSettings/path");
      wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
      if (!wizDefVal.length())
        wizDefVal.append(STANDARD_COMPFILESDIR);

      wizDefVal.append(PATHSEPSTR"componentfiles");
    }
    else if (!strcmp(attr.queryProp(tempPath.str()), "$processname"))
    {
      tempPath.clear().appendf("Software/%s[1]/@name",compName.str());
      wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
    }
    else if(!strcmp(attr.queryProp(tempPath.str()), "$hthorcluster"))
    {
      tempPath.clear().append(XML_TAG_SOFTWARE "/" XML_TAG_TOPOLOGY "/" XML_TAG_CLUSTER);
      Owned<IPropertyTreeIterator> iterClusters = m_pEnv->getElements(tempPath.str());
      ForEach (*iterClusters)
      {
        IPropertyTree* pCluster = &iterClusters->query();

        if (pCluster->queryPropTree(XML_TAG_ROXIECLUSTER) ||
            pCluster->queryPropTree(XML_TAG_THORCLUSTER))
          continue;
        else
        {
          wizDefVal.clear().append(pCluster->queryProp(XML_ATTR_NAME));
          break;
        }
      }
    }
    else
    {
      wizDefVal.clear().append(attr.queryProp(tempPath.str()));
      tempPath.clear().appendf("Software/%s[1]/@buildSet", compName.str());
      if (m_pEnv->queryProp(tempPath.str()))
      {
        SWProcess * swp = (SWProcess *)m_eh->getEnvSWComp(m_buildSetName.str());
        if (((SWProcess *)m_eh->getEnvSWComp(m_buildSetName.str()))->getInstanceCount() > 1)
        {
          tempPath.clear().append("./xs:annotation/xs:appinfo/autogendefaultformultinode");
          if (attr.hasProp(tempPath.str()))
            wizDefVal.clear().append(attr.queryProp(tempPath.str()));
        }
      }
    }
  }
  else if(attr.hasProp("./xs:annotation/xs:appinfo/autogenprefix") ||
          attr.hasProp("./xs:annotation/xs:appinfo/autogensuffix"))
  {
    StringBuffer sb;
    StringBuffer nameOfComp;
    tempPath.clear().appendf("./Software/%s[1]/@name",m_compName.str());
    nameOfComp.clear().append(m_pEnv->queryProp(tempPath.str()));

    tempPath.clear().append("./xs:annotation/xs:appinfo/autogenprefix");
    if (attr.hasProp(tempPath.str()))
      sb.clear().append(attr.queryProp(tempPath.str())).append(nameOfComp);

    tempPath.clear().append("./xs:annotation/xs:appinfo/autogensuffix");
    if (attr.hasProp(tempPath.str()))
    {
      if (sb.length())
        sb.append(attr.queryProp(tempPath.str()));
      else
        sb.append(nameOfComp).append(attr.queryProp(tempPath.str()));
    }

    wizDefVal.clear().append(sb);
  }
  else if (!strcmp(type,"computerType"))
  {
    if (m_wizFlag)
    {
      StringBuffer ipAddr;
      tempPath.clear().appendf("./Programs/Build/BuildSet[%s=\"%s\"]",XML_ATTR_PROCESS_NAME,m_compName.str());
      IPropertyTree* pCompTree = m_pEnv->queryPropTree(tempPath.str());
      if (pCompTree)
      {
        StringArray ipArray;
        ((SWProcess *)m_eh->getEnvSWComp(m_buildSetName.str()))->getInstanceNetAddresses(ipArray);
        if ( ipArray.ordinality() > 0 )
        {
          ForEachItemIn(x, ipArray)
          {
            if (ipArray.ordinality() == 1)
              ipAddr.append(ipArray.item(x));
            else
              ipAddr.append(ipArray.item(x)).append(",");

            tempPath.clear().appendf("./Hardware/Computer[@netAddress=\"%s\"]",ipAddr.str());
            IPropertyTree* pHard = m_pEnv->queryPropTree(tempPath.str());
            if (pHard)
            {
              tempPath.clear().append("@name");
              wizDefVal.clear().append(pHard->queryProp(tempPath.str()));
            }
          }
        }
      }
    }
  }
  else if(!strcmp(type,"serverListType"))
  {
    if (m_wizFlag)
    {
      StringBuffer ipAddr;
      tempPath.clear().appendf("./Programs/Build/BuildSet[%s=\"%s\"]",XML_ATTR_PROCESS_NAME, m_compName.str());
      IPropertyTree* pCompTree = m_pEnv->queryPropTree(tempPath.str());
      if (pCompTree)
      {
        StringArray ipArray;
        ((SWProcess*)m_eh->getEnvSWComp(m_buildSetName.str()))->getInstanceNetAddresses(ipArray);
        if ( ipArray.ordinality() > 0 )
        {
           wizDefVal.clear().append(ipArray.item(0));
        }
      }
    }
  }
  else if(!strcmp(type,"xs:string"))
  {
    StringBuffer nameOfComp;
    tempPath.clear().appendf("./Software/%s[1]/@name",m_compName.str());
    nameOfComp.clear().append(m_pEnv->queryProp(tempPath.str()));

    if (!strcmp(name, "dbUser"))
    {
      wizDefVal.clear().append(m_eh->getConfig("dbuser", CONFIG_ALL));
    }
    else if (!strcmp(name, "dbPassword"))
    {
       wizDefVal.clear().append(m_eh->getConfig("dbpassword", CONFIG_ALL));
    }
  }
  else if (!strcmp(type,"mysqlType"))
  {
    tempPath.clear().append("./Software/MySQLProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type,"espprocessType"))
  {
    tempPath.clear().append("./Software/EspProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type,"mysqlloggingagentType"))
  {
    tempPath.clear().append("./Software/MySQLLoggingAgent[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type,"esploggingagentType"))
  {
    tempPath.clear().append("./Software/ESPLoggingAgent[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type,"loggingmanagerType"))
  {
    tempPath.clear().append("./Software/LoggingManager[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type,"daliServersType"))
  {
    tempPath.clear().append("./Software/DaliServerProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type,"ldapServerType"))
  {
    tempPath.clear().append("./Software/LdapServerProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type, "roxieClusterType"))
  {
    tempPath.clear().append("./Software/RoxieCluster[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type, "eclServerType"))
  {
    tempPath.clear().append("./Software/EclServerProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type, "eclCCServerType"))
  {
    tempPath.clear().append("./Software/EclCCServerProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type, "espProcessType"))
  {
    tempPath.clear().append("./Software/EspProcess[1]/@name");
    wizDefVal.clear().append(m_pEnv->queryProp(tempPath.str()));
  }
  else if (!strcmp(type, "espBindingType"))
  {
    IPropertyTree* pAppInfo = attr.queryPropTree("xs:annotation/xs:appinfo");
    StringBuffer xpath;
    const char* serviceType = NULL;
    if (pAppInfo)
    {
      serviceType = pAppInfo->queryProp("serviceType");
      if (serviceType)
        xpath.clear().append("Software/EspService");
    }
    else
      xpath.clear().appendf("Software/*[@buildSet='%s']", type);

    wizDefVal.clear();

    Owned<IPropertyTreeIterator> iterEspServices = m_pEnv->getElements(xpath);
    short blank = 0;
    ForEach (*iterEspServices)
    {
      IPropertyTree* pEspService = &iterEspServices->query();

      if (serviceType)
      {
        xpath.clear().appendf("Properties[@type='%s']", serviceType);
        if (pEspService->queryPropTree(xpath.str()) == NULL)
          continue;
      }

      Owned<IPropertyTreeIterator> iter = m_pEnv->getElements("Software/EspProcess[1]");

      ForEach (*iter)
      {
        IPropertyTree* pEspProcess = &iter->query();
        xpath.clear().appendf("EspBinding[@service='%s']", pEspService->queryProp(XML_ATTR_NAME));
        if (pEspProcess->queryPropTree(xpath.str()) != NULL)
          wizDefVal.append(pEspProcess->queryProp(XML_ATTR_NAME)).append("/").append(pEspService->queryProp(XML_ATTR_NAME));
      }
    }
  }
  else if (!strcmp(type, "ipAddressAndPort"))
  {
    StringBuffer defaultPort;
    tempPath.clear().append("./xs:annotation/xs:appinfo/defaultPort");
    defaultPort.append(attr.queryProp(tempPath.str()));
    tempPath.clear().append("./xs:annotation/xs:appinfo/autogenxpath");
    if (attr.hasProp(tempPath.str()))
    {
      StringBuffer computerName;
      computerName.append(m_pEnv->queryProp(attr.queryProp(tempPath.str())));
      tempPath.clear().appendf("./Hardware/Computer[@name=\"%s\"]",computerName.str());
      if (m_pEnv->hasProp(tempPath.str()))
      {
        IPropertyTree* pHard = m_pEnv->queryPropTree(tempPath.str());
        if (pHard)
          wizDefVal.clear().append(pHard->queryProp("./" XML_ATTR_NETADDRESS)).append(":").append(defaultPort);
      }
    }
  }
}

}
