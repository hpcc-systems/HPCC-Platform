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
#define verify(p) ((void) (p))
#define ForEach(i) for((i).first();(i).isValid();(i).next())


namespace HPCCPhoneNumber
{

IPluginContext * parentCtx = NULL;

ECL_PHONENUMBER_API bool ECL_PHONENUMBER_CALL checkValidity(ICodeContext *_ctx, size32_t lenNumber, const char *number, size32_t lenCountryCode, const char *countryCode)
{
    const i18n::phonenumbers::PhoneNumberUtil* phoneUtil = i18n::phonenumbers::PhoneNumberUtil::GetInstance();
    i18n::phonenumbers::PhoneNumber phoneNumber;
    try
    {
        std::string numberStr(number, lenNumber);
        std::string countryCodeStr(countryCode, lenCountryCode);
        phoneUtil->Parse(numberStr, countryCodeStr, &phoneNumber);
        return phoneUtil->IsValidNumber(phoneNumber);
    }
    catch (...)
    {
        return false;
    }
}

ECL_PHONENUMBER_API void ECL_PHONENUMBER_CALL phonenumberType(ICodeContext *_ctx, size32_t &lenResult, char *&result, size32_t lenNumber, const char *number, size32_t lenCountryCode, const char *countryCode)
{
    const i18n::phonenumbers::PhoneNumberUtil* phoneUtil = i18n::phonenumbers::PhoneNumberUtil::GetInstance();
    i18n::phonenumbers::PhoneNumber phoneNumber;
    std::string resultStr;
    
    try
    {
        std::string numberStr(number, lenNumber);
        std::string countryCodeStr(countryCode, lenCountryCode);
        phoneUtil->Parse(numberStr, countryCodeStr, &phoneNumber);
        i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType type = phoneUtil->GetNumberType(phoneNumber);
        switch (type)
        {
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::FIXED_LINE:
            resultStr = "FIXED_LINE";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::MOBILE:
            resultStr = "MOBILE";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::FIXED_LINE_OR_MOBILE:
            resultStr = "FIXED_LINE_OR_MOBILE";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::TOLL_FREE:
            resultStr = "TOLL_FREE";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::PREMIUM_RATE:
            resultStr = "PREMIUM_RATE";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::SHARED_COST:
            resultStr = "SHARED_COST";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::VOIP:
            resultStr = "VOIP";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::PERSONAL_NUMBER:
            resultStr = "PERSONAL_NUMBER";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::PAGER:
            resultStr = "PAGER";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::UAN:
            resultStr = "UAN";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::VOICEMAIL:
            resultStr = "VOICEMAIL";
            break;
        case i18n::phonenumbers::PhoneNumberUtil::PhoneNumberType::UNKNOWN:
            resultStr = "UNKNOWN";
            break;
        default:
            resultStr = "INVALID";
            break;
        }
    }
    catch (...)
    {
        resultStr = "INVALID";
    }
    
    lenResult = resultStr.length();
    result = static_cast<char *>(rtlMalloc(lenResult));
    memcpy(result, resultStr.c_str(), lenResult);
}

ECL_PHONENUMBER_API void ECL_PHONENUMBER_CALL regionCode(ICodeContext *_ctx, size32_t &lenResult, char *&result, size32_t lenNumber, const char *number, size32_t lenCountryCode, const char *countryCode)
{
    const i18n::phonenumbers::PhoneNumberUtil* phoneUtil = i18n::phonenumbers::PhoneNumberUtil::GetInstance();
    i18n::phonenumbers::PhoneNumber phoneNumber;
    std::string regionCodeStr;
    
    try
    {
        std::string numberStr(number, lenNumber);
        std::string countryCodeStr(countryCode, lenCountryCode);
        phoneUtil->Parse(numberStr, countryCodeStr, &phoneNumber);
        phoneUtil->GetRegionCodeForNumber(phoneNumber, &regionCodeStr);
    }
    catch (...)
    {
        regionCodeStr = "";
    }
    
    lenResult = regionCodeStr.length();
    result = static_cast<char *>(rtlMalloc(lenResult));
    if (lenResult > 0) {
        memcpy(result, regionCodeStr.c_str(), lenResult);
    }
}

ECL_PHONENUMBER_API unsigned ECL_PHONENUMBER_CALL countryCode(ICodeContext *_ctx, size32_t lenNumber, const char *number, size32_t lenCountryCode, const char *countryCode)
{
    const i18n::phonenumbers::PhoneNumberUtil* phoneUtil = i18n::phonenumbers::PhoneNumberUtil::GetInstance();
    i18n::phonenumbers::PhoneNumber phoneNumber;
    try
    {
        std::string numberStr(number, lenNumber);
        std::string countryCodeStr(countryCode, lenCountryCode);
        phoneUtil->Parse(numberStr, countryCodeStr, &phoneNumber);
        return phoneNumber.country_code();
    }
    catch (...)
    {
        return 0;
    }
}

} // namespace HPCCPhoneNumber