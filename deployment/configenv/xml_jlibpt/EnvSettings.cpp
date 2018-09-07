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

#include "EnvSettings.hpp"
#include "deployutils.hpp"

namespace ech
{


EnvSettings::EnvSettings(EnvHelper * envHelper):ComponentBase("envsettings", envHelper)
{
}

void EnvSettings::create(IPropertyTree *params)
{
  IPropertyTree * envTree = m_envHelper->getEnvTree();
  assert (envTree);

  IPropertyTree* pSettings = envTree->addPropTree(XML_TAG_ENVSETTINGS, createPTree());

  const EnvConfigOptions& cfgOptions = m_envHelper->getEnvConfigOptions();
  const IProperties * pCfgOptions = cfgOptions.getProperties();
  Owned<IPropertyIterator> iter = pCfgOptions->getIterator();

  ForEach(*iter)
  {
    StringBuffer prop;
    pCfgOptions->getProp(iter->getPropKey(), prop);
    pSettings->addProp(iter->getPropKey(), prop.length() ? prop.str():"");
  }

}


unsigned EnvSettings::add(IPropertyTree *params)
{
   return 0;
}

void EnvSettings::modify(IPropertyTree *params)
{
   return;
}

void EnvSettings::remove(IPropertyTree *params)
{
   return;
}

}
