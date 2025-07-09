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

enum phoneNumberType : __int8
{
    FIXED_LINE,
    MOBILE,
    FIXED_LINE_OR_MOBILE,
    TOLL_FREE,
    PREMIUM_RATE,
    SHARED_COST,
    VOIP,
    PERSONAL_NUMBER,
    PAGER,
    UAN,
    VOICEMAIL,
    UNKNOWN
};

static void throwOnError(i18n::phonenumbers::PhoneNumberUtil::ErrorType err)
{
    switch (err)
    {
    case i18n::phonenumbers::PhoneNumberUtil::ErrorType::NO_PARSING_ERROR:
        return;
    case i18n::phonenumbers::PhoneNumberUtil::ErrorType::INVALID_COUNTRY_CODE_ERROR:
        throw MakeStringException(err, "libphonenumber : Invalid country code");
    case i18n::phonenumbers::PhoneNumberUtil::ErrorType::NOT_A_NUMBER:
        throw MakeStringException(err, "libphonenumber : Not a number");
    case i18n::phonenumbers::PhoneNumberUtil::ErrorType::TOO_SHORT_AFTER_IDD:
        throw MakeStringException(err, "libphonenumber : Too short after IDD");
    case i18n::phonenumbers::PhoneNumberUtil::ErrorType::TOO_SHORT_NSN:
        throw MakeStringException(err, "libphonenumber : Too short NSN");
    case i18n::phonenumbers::PhoneNumberUtil::ErrorType::TOO_LONG_NSN:
        throw MakeStringException(err, "libphonenumber : Too long NSN");
    default:
        throw MakeStringException(err, "libphonenumber : Unknown error");
    }
}

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
    throwOnError(phoneUtil->Parse(numberStr, countryCodeStr, &phoneNumber));

    // Format the results
    std::string formattedNumber;
    phoneUtil->Format(phoneNumber, i18n::phonenumbers::PhoneNumberUtil::PhoneNumberFormat::E164, &formattedNumber);
    
    bool isValid = phoneUtil->IsValidNumber(phoneNumber);
    __int8 lineType = static_cast<__int8>(phoneUtil->GetNumberType(phoneNumber));
    
    std::string regionCode;
    phoneUtil->GetRegionCodeForNumber(phoneNumber, &regionCode);
    __int16 countryCodeInt = phoneNumber.country_code();

    size32_t regionCodeLen = regionCode.size();
    size32_t formattedNumberLen = formattedNumber.size();

    mb.append(formattedNumberLen).append(formattedNumberLen, formattedNumber.c_str());
    mb.append(isValid);
    mb.append(lineType);
    mb.append(regionCodeLen).append(regionCodeLen, regionCode.c_str());
    mb.append(countryCodeInt);

    __lenResult = mb.length();
    __result = mb.detach();
}

} // namespace phonenumber