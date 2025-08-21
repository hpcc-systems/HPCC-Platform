/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#include "platform.h"
#include "eclrtl.hpp"
#include "jexcept.hpp"
#include "phonenumber.hpp"
#include <string>
#define PHONENUMBER_VERSION "phonenumber plugin 1.0.0"

ECL_PHONENUMBER_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb)
{
    /*  Warning:    This function may be called without the plugin being loaded fully.
     *              It should not make any library calls or assume that dependent modules
     *              have been loaded or that it has been initialised.
     *
     *              Specifically:  "The system does not call DllMain for process and thread
     *              initialization and termination.  Also, the system does not load
     *              additional executable modules that are referenced by the specified module."
     */

    if (pb->size != sizeof(ECLPluginDefinitionBlock))
        return false;

    pb->magicVersion = PLUGIN_VERSION;
    pb->version = PHONENUMBER_VERSION;
    pb->moduleName = "lib_phonenumber";
    pb->ECL = NULL;
    pb->flags = PLUGIN_IMPLICIT_MODULE;
    pb->description = "ECL plugin library for google libphonenumber";
    return true;
}

// ForEach & verify macro is defined in eclhelper.hpp and causes a conflict with the PhoneNumberUtil class
#undef ForEach
#undef verify
#include <phonenumbers/phonenumberutil.h>

namespace phonenumber
{
//--------------------------------------------------------------------------------
//                           ECL SERVICE ENTRYPOINTS
//--------------------------------------------------------------------------------

ECL_PHONENUMBER_API void ECL_PHONENUMBER_CALL parser(ICodeContext *_ctx, size32_t &__lenResult, void *&__result, size32_t lenNumber, const char *number, size32_t lenCountryCode, const char *countryCode)
{
    MemoryBuffer mb;
    i18n::phonenumbers::PhoneNumber phoneNumber;
    const i18n::phonenumbers::PhoneNumberUtil* phoneUtil = i18n::phonenumbers::PhoneNumberUtil::GetInstance();
    std::string numberStr(number, lenNumber);
    std::string countryCodeStr(countryCode, lenCountryCode);
    // Set default values that would normally returned from calls to the PhoneNumber object if the number is invalid
    std::string formattedNumber = "+00";
    std::string regionCode = "ZZ";
    __int16 countryCodeInt = 0;
    __int8 lineType = 11;
    bool isValid = false;

    i18n::phonenumbers::PhoneNumberUtil::ErrorType _parseError = phoneUtil->Parse(numberStr, countryCodeStr, &phoneNumber);
    if(_parseError == i18n::phonenumbers::PhoneNumberUtil::ErrorType::NO_PARSING_ERROR)
    {
        phoneUtil->Format(phoneNumber, i18n::phonenumbers::PhoneNumberUtil::PhoneNumberFormat::E164, &formattedNumber);
        isValid = phoneUtil->IsValidNumber(phoneNumber);
        lineType = static_cast<__int8>(phoneUtil->GetNumberType(phoneNumber));
        phoneUtil->GetRegionCodeForNumber(phoneNumber, &regionCode);
        countryCodeInt = phoneNumber.country_code();
    }

    size32_t regionCodeLen = regionCode.size();
    size32_t formattedNumberLen = formattedNumber.size();
    __int8 err = static_cast<__int8>(_parseError);

    mb.append(formattedNumberLen).append(formattedNumberLen, formattedNumber.c_str());
    mb.append(err);
    mb.append(isValid);
    mb.append(lineType);
    mb.append(regionCodeLen).append(regionCodeLen, regionCode.c_str());
    mb.append(countryCodeInt);

    __lenResult = mb.length();
    __result = mb.detach();
}

} // namespace phonenumber