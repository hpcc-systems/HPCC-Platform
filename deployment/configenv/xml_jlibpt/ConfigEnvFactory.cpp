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

#include "IConfigEnv.hpp"
#include "ConfigEnvFactory.hpp"

namespace ech
{

IConfigEnv<IPropertyTree,StringBuffer> * ConfigEnvFactory::getIConfigEnv(IPropertyTree *config)
{
    return  new ConfigEnv(config);
}

void ConfigEnvFactory::destroy( IConfigEnv<IPropertyTree,StringBuffer> * iCfgEnv)
{
    ConfigEnv * cfgEnv = (ConfigEnv *) iCfgEnv;
    delete cfgEnv;
}

}

