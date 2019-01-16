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

#include "IPAddressRangeVariable.hpp"
#include "TemplateException.hpp"
#include "Utils.hpp"

void IPAddressRangeVariable::addValue(const std::string &range)
{
    //
    // Formats accepted:
    //  iplist = ip[;ip]
    //  ip = a.b.c.d | a.b.c.g-h | a.b.c.d*10

    m_values.clear();
    std::vector<std::string> ipDefs = splitString(range, ";");
    for (auto &ipDef: ipDefs)
    {
        std::string def = trim(ipDef);
        bool isValid = true;

        //
        // Make sure there are 4 parts to the address
        std::vector<std::string> addressParts = splitString(def, ".");
        if (addressParts.size() == 4)
        {
            std::string ipAdressBase;
            //
            // check to make sure the first 3 parts are valid numbers
            for (unsigned i=0; i<3 && isValid; ++i)
            {
                try
                {
                    int num = std::stoi(addressParts[i]);
                    if (num <= 0 || num > 255)
                    {
                        isValid = false;
                    }
                    else
                    {
                        ipAdressBase += addressParts[i] + ".";
                    }
                }
                catch (...)
                {
                    isValid = false;
                }
            }

            //
            // If valid, look at the last part, it's one of three possibilities
            // 1. a*b - indicats starting 4th octet value 'a' and creating 'b' IP addresses
            // 2. a-b - indicates create a range of ip addresses with the 4th octet ranging from a to b
            // 3. a   - just a single octet, so a single ip address
            if (isValid)
            {
                int start, stop;
                if (addressParts[3].find('*') != std::string::npos)
                {
                    std::vector<std::string> rangeParts = splitString(addressParts[3], "*");
                    try
                    {
                        start = std::stoi(rangeParts[0]);
                        int num = std::stoi(rangeParts[1]);
                        if (start <= 0 || start > 255 || num <=0 || (num+start) > 255)
                        {
                            isValid = false;
                        }
                        else
                        {
                            for (int i=0; i<num; ++i)
                            {
                                m_values.emplace_back(ipAdressBase + std::to_string(start + i));
                            }
                        }
                    }
                    catch (...)
                    {
                        isValid = false;
                    }

                }
                else if (addressParts[3].find('-') != std::string::npos)
                {
                    std::vector<std::string> rangeParts = splitString(addressParts[3], "-");
                    if (rangeParts.size() == 2)
                    {
                        try
                        {
                            start = std::stoi(rangeParts[0]);
                            stop = std::stoi(rangeParts[1]);
                            if (start <= 0 || start > 255 || stop <= 0 || stop > 255)
                            {
                                isValid = false;
                            }
                            else
                            {
                                for (int oct = start; oct <= stop; ++oct)
                                {
                                    m_values.emplace_back(ipAdressBase + std::to_string(oct));
                                }
                            }
                        }
                        catch (...)
                        {
                            isValid = false;
                        }
                    }
                    else
                    {
                        isValid = false;
                    }
                }
                else
                {
                    try
                    {
                        int num = std::stoi(addressParts[3]);
                        if (num <= 0 || num > 255)
                        {
                            isValid = false;
                        }
                        else
                        {
                            m_values.emplace_back(ipDef);
                        }
                    }
                    catch (...)
                    {
                        isValid = false;
                    }
                }
            }
        }
        else
        {
            isValid = false;
        }

        if (!isValid)
        {
            throw TemplateException("Invalid IP address range (" + range + ") specified", false);
        }
    }
}
