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

#include "IPAddressInput.hpp"
#include "TemplateException.hpp"
#include "Utils.hpp"

void IPAddressInput::setValue(const std::string &value) {

    std::string ipAddr = trim(value);
    bool isValid = true;

    //
    // Figure out what this def entails. Look at the number of parts. If one part, just
    // save it.
    std::vector<std::string> addressParts = splitString(ipAddr, ".");
    if (addressParts.size() == 4)
    {
        for (unsigned i = 0; i < 4 && isValid; ++i)
        {
            try
            {
                int num = std::stoi(addressParts[i]);
                if (num <= 0 || num > 255)
                {
                    isValid = false;
                }
            }
            catch (...)
            {
                isValid = false;
            }
        }
    }
    else
    {
        isValid = false;
    }

    if (!isValid)
    {
        throw TemplateException("Invalid IP address '" + ipAddr + "' specified", false);
    }

    m_values.emplace_back(ipAddr);
}
