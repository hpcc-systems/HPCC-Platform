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
#ifndef _COMPONENTFROMXSD_HPP_
#define _COMPONENTFROMXSD_HPP_

//#define TRACE_SCHEMA_NODE(msg, schemaNode)

//#define CONFIGMGR_JSPATH "./"
//#define STANDARD_COMPFILESDIR INSTALL_DIR

//#define STANDARD_CONFIGXMLDIR COMPONENTFILES_DIR"/configxml"

namespace ech
{

class EnvHelper;


class ComponentFromXSD
{
 public:
   ComponentFromXSD(EnvHelper* peh);
   ~ComponentFromXSD();

  void CreateAttributeFromSchema(IPropertyTree& attr,
         StringBuffer compName, const char* childElementName);
  void AddAttributeFromSchema(IPropertyTree& schemaNode,
         StringBuffer elemName, StringBuffer& compName, const char* childElementName);
  void AddAttributesFromSchema(IPropertyTree* pSchema,
         StringBuffer& compName, const char* childElementName);
  void ProcessElementSchemaNode(IPropertyTree* pElement,
         IPropertyTree* pParentElement, StringBuffer& sbCompName);
  void ProcessComplexTypeSchemaNode(IPropertyTree* schemaNode,
         IPropertyTree* pParentElement, StringBuffer& sbCompName);
  bool generateHeaders();
  void setCompTree(const char* buildSetName,  IPropertyTree* pTree, IPropertyTree* schemaTree, bool allSubTypes);
  void setWizardFlag(bool flag) { m_wizFlag = flag; }
  void setGenerateOptional(bool flag) { m_genOptional = flag; }
  void getValueForTypeInXSD(IPropertyTree& attr, StringBuffer compName, StringBuffer& wizDefVal);

private:

  Linked<const IPropertyTree> m_pEnv;
  Linked<IPropertyTree> m_pSchemaRoot;
  IPropertyTree* m_pCompTree;
  IPropertyTree* m_pDefTree;
  //Owned<IPropertyTree> m_viewChildNodes;
  //Owned<IPropertyTree> m_multiRowNodes;
  StringBuffer m_buildSetName;
  StringBuffer m_compName;
  StringBuffer m_xpathDefn;
  short m_numAttrs;
  bool m_allSubTypes;
  bool m_wizFlag;
  bool m_genOptional;
  EnvHelper* m_eh;
};

}
#endif
