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

#include "jiface.hpp"
#include "jliball.hpp"
#include "GenEnvRules.hpp"

namespace ech
{

void GenEnvRules::loadFile(const char* filename)
{
  rules.setown(createProperties(filename));

  if (rules == NULL)
    MakeStringException(-1, "Cannot load file %s", filename);

  const char* roxieRedTypes[] = {"Full", "Circular", "None", "Overloaded"};
  roxieAgentRedType.clear().append("Circular");
  roxieAgentRedChannels = 2;
  roxieAgentRedOffset = 1;

  //To do CConfigHelper::getInstance()->addPluginsToGenEnvRules(m_algProp.get());

  Owned<IPropertyIterator> iter = rules->getIterator();
  StringBuffer prop;
  ForEach(*iter)
  {
    rules->getProp(iter->getPropKey(), prop.clear());
    if (prop.length() && prop.charAt(prop.length()-1) == ',')
       prop.setCharAt((prop.length()-1),' ');

    if (!strcmp(iter->getPropKey(), "roxie_agent_redundancy"))
    {
      StringArray sbarr;
      sbarr.appendList(prop.str(), ",");
      if (sbarr.length() > 1)
      {
        int type = atoi(sbarr.item(0));
        if (type == 0)
          continue;

        if (type > 0 && type < 5)
          roxieAgentRedType.clear().append(roxieRedTypes[type-1]);
        else
          continue;

        roxieAgentRedChannels = atoi(sbarr.item(1));
        if (roxieAgentRedChannels <= 0)
          roxieAgentRedChannels = 1;

        if (sbarr.length() > 2)
        {
          roxieAgentRedOffset = atoi(sbarr.item(2));
          if (roxieAgentRedOffset <= 0)
            roxieAgentRedOffset = 1;
        }
        else
          roxieAgentRedOffset = 0;
      }
    }
  }
}

int GenEnvRules::getPropInt(const char* propname, int dft) const
{
  StringBuffer prop;
  rules->getProp(propname, prop.clear());
  if (prop.length() > 0)
    return atoi(prop.str());

  return dft;
}

const char* GenEnvRules::getProp(const char* propname) const
{
  return rules->queryProp(propname);
}

bool GenEnvRules::doNotGenTheCompOptional(const char* compname) const
{
  if (foundInProp("do_not_gen_optional", compname) &&
      foundInProp("do_not_gen_optional", "all"))
    return true;

  return false;
}

bool GenEnvRules::foundInProp(const char* propname, const char* item, const char* sep) const
{
  StringBuffer prop;
  rules->getProp(propname, prop.clear());
  if (prop.length() == 0)
    return false;

  StringArray propvalue;
  propvalue.appendList(prop.str(), sep);
  if (propvalue.find(item) != NotFound)  return true;

  return false;
}

bool GenEnvRules::isValidServerCombo(const char* server1, const char* server2) const
{
   StringBuffer s1, s2;
   s1.clear().appendf("%s-%s",server1, server2);
   s2.clear().appendf("%s-%s",server2, server1);

   const char * propname = "avoid_combo";

   if (!foundInProp(propname, s1.str()) && !foundInProp(propname, s1.str()))
     return true;

   return false;;
}

}
