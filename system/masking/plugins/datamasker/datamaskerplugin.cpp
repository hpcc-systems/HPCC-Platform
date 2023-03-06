/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2022 HPCC Systems®.

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

#include "datamaskerplugin.hpp"

extern "C"
{
    /**
     * The plugin passes a default instance of CModuleFactory to a new instance of
     * CEspLogAgent. This makes all platform-native modules available for use.
     *
     * The README.md file must be updated if the default factory is updated to include more modules
     * or if plugin-specific modules are defined and registered.
     */
    DATAMASKERPLUGIN_API IDataMaskingProfileIterator* newPartialMaskSerialToken(const IPTree& configuration, ITracer& tracer)
    {
        using MaskStyle = DataMasking::CPartialMaskStyle;
        using Rule = DataMasking::CSerialTokenRule;
        using ValueType = DataMasking::TValueType<MaskStyle, Rule>;
        using Context = DataMasking::CContext;
        using Profile = DataMasking::TSerialProfile<ValueType, Rule, Context>;
        using Plugin = DataMasking::TPlugin<Profile>;

        Owned<Plugin> plugin(new Plugin(tracer));
        if (plugin)
            (void)plugin->configure(configuration);
        return plugin.getClear();
    }
}
