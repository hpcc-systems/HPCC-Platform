/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#ifndef ESDL_SUMMARY_PROFILE_HPP
#define ESDL_SUMMARY_PROFILE_HPP

#include "jiface.hpp"
#include "jstring.hpp"
#include "esp.hpp"
#include "txsummary.hpp"

// Contains a list of mappings used to rename a TxEntry when being output
// under conditions that match the group and style in the mapping.
//
// Each mapping has: original_name, group_flags, style_flags, new_name
//
// Serialization always specifies outputGroup flags and outputStyle flags.
// Use the mapped new name to output when:
//      group_flags & outputGroup == true and style_flags & outputStyle == true
//

class CTxSummaryProfileEsdl : public CTxSummaryProfileBase
{
    public:
        virtual bool tailorSummary(IEspContext* ctx) override;

    private:
        unsigned int parseSystemInfo(const char* name, StringBuffer& sysinfo);
        void configure();
};

#endif //ESDL_SUMMARY_PROFILE_HPP
