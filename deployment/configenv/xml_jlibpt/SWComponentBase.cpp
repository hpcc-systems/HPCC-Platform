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
#include "SWComponentBase.hpp"
#include "deployutils.hpp"
#include "configenvhelper.hpp"
#include "buildset.hpp"
#include "ComponentFromXSD.hpp"
#include "Hardware.hpp"

namespace ech
{

SWComponentBase::SWComponentBase(const char* name, EnvHelper * envHelper):ComponentBase(name, envHelper)
{
  StringBuffer xpath, buildSetPath;
  xpath.clear().appendf("./%s/%s/%s/[@name=\"%s\"]", XML_TAG_PROGRAMS, XML_TAG_BUILD, XML_TAG_BUILDSET, name);
  const IPropertyTree * buildSetTree = m_envHelper->getBuildSetTree();
  m_pBuildSet = buildSetTree->queryPropTree(xpath.str());
  if (m_pBuildSet == NULL)
    throw MakeStringException(CfgEnvErrorCode::InvalidParams, "Cannot find buildset name %s. Beware it is case sensitive.", name);


  // path attributes from buildset.xml are not correct.
  m_pSchema.setown(loadSchema(buildSetTree->queryPropTree("./" XML_TAG_PROGRAMS "/" XML_TAG_BUILD "[1]"),
    m_pBuildSet, buildSetPath, NULL));

  m_buildName.clear().appendf("%s", m_envHelper->getBuildSetTree()->queryPropTree("./Programs/Build[1]")->queryProp(XML_ATTR_NAME));
  m_xsdFileName.clear().appendf("%s", m_pBuildSet->queryProp(XML_ATTR_SCHEMA));
  m_buildSetName.clear().appendf("%s", m_pBuildSet->queryProp(XML_ATTR_NAME));
  m_processName.clear().appendf("%s", m_pBuildSet->queryProp(XML_ATTR_PROCESS_NAME));

  assert(m_processName && *m_processName);
  assert(m_xsdFileName && *m_xsdFileName);

}

SWComponentBase::~SWComponentBase()
{
  m_pSchema.clear();
}

IPropertyTree * SWComponentBase::addComponent(IPropertyTree *params)
{
  const char* clone = params->queryProp("@clone");
  if (clone)
  {
     return ComponentBase::cloneComponent(params);
  }

  const char* key = params->queryProp("@key");
  StringBuffer sbCompName;
  sbCompName.clear().append(key);

  IPropertyTree * envTree = m_envHelper->getEnvTree();

  StringBuffer deployable  = m_pBuildSet->queryProp("@" TAG_DEPLOYABLE);

  IPropertyTree * pt = NULL;

  Owned<IPropertyTree> pCompTree(createPTree(m_processName));

  ComponentFromXSD cfx(m_envHelper);
  cfx.setCompTree(m_name.str(), pCompTree, m_pSchema, false);
  cfx.setWizardFlag(false);
  cfx.setGenerateOptional(true);
  cfx.generateHeaders();


  StringBuffer sbNewName;
  sbNewName.clear();
  sbNewName = sbCompName;
  if (sbCompName.isEmpty() || !strcmp(sbCompName.str(), "my"))
  {
    StringBuffer sName(m_buildSetName);
    sName.toLowerCase();
    sName.replaceString("process","");
    sbNewName.append(sbCompName.str()).append(getUniqueName(envTree, sName, m_processName.str(), "Software"));
  }
  else
    sbNewName = sbCompName;

  pCompTree->setProp(XML_ATTR_NAME, sbNewName);
  pCompTree->setProp(XML_ATTR_BUILD,   m_buildName.str());
  pCompTree->setProp(XML_ATTR_BUILDSET,m_buildSetName.str());


  Owned<IPropertyTree> pProperties = m_pBuildSet->getPropTree("Properties");
  if (pProperties)
    pCompTree->addPropTree("Properties", createPTreeFromIPT(pProperties));


  IPropertyTree* pSoftware = envTree->queryPropTree(XML_TAG_SOFTWARE);

  synchronized block(mutex);
  pSoftware->addPropTree(m_processName, pCompTree);

  return pCompTree.getLink();
}

void SWComponentBase::create(IPropertyTree *params)
{
  IPropertyTree * envTree = m_envHelper->getEnvTree();

  StringBuffer sbTask;
  sbTask.appendf("<Task component=\"%s\" key=\"my%s\"/>", m_name.str(), m_name.str());

  Owned<IPropertyTree> task = createPTreeFromXMLString(sbTask.str());
  IPropertyTree * pCompTree = addComponent(task);
  assert(pCompTree);

}


unsigned SWComponentBase::add(IPropertyTree *params)
{
   IPropertyTree * envTree = m_envHelper->getEnvTree();
   const char* key = params->queryProp("@key");
   StringBuffer xpath;
   xpath.clear().appendf("%s/%s[@name=\"%s\"]", XML_TAG_SOFTWARE, m_processName.str(), key);
   IPropertyTree * compTree = envTree->queryPropTree(xpath.str());
   if (!compTree)
   {
      compTree = addComponent(params);
      assert(compTree);
   }

   return 0;
}


void SWComponentBase::modify(IPropertyTree *params)
{
   ComponentBase::modify(params);
}


}
