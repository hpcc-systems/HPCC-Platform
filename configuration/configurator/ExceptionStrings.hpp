/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems®.

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

#ifndef _EXCEPTION_STRINGS_HPP_
#define _EXCEPTION_STRINGS_HPP_

#include "jexcept.hpp"
#include <cstring>

#define MAX_EXCEPTION_STRING_LENGTH 2048

#define CATCH_EXCEPTION_AND_EXIT \
catch (IException *except) \
{\
    StringBuffer strErrMsg;\
    except->errorMessage(strErrMsg);\
    std::cout << std::endl << strErrMsg.str() << std::endl << std::endl;\
    exit(-1);\
}

enum eExceptionCodes
{
    EX_STR_CAN_NOT_OPEN_XSD  = 1,
    EX_STR_SIMPLE_TYPE_ALREADY_DEFINED,
    EX_STR_COMPLEX_TYPE_ALREADY_DEFINED,
    EX_STR_ATTRIBUTE_GROUP_ALREADY_DEFINED,
    EX_STR_CAN_NOT_PROCESS_ENV_XML,
    EX_STR_XPATH_DOES_NOT_EXIST_IN_TREE,
    EX_STR_MISSING_REQUIRED_ATTRIBUTE,
    EX_STR_MISSING_VALUE_ATTRIBUTE_IN_LENGTH,
    EX_STR_LENGTH_VALUE_MUST_BE_GREATER_THAN_OR_EQUAL_TO_ZERO,
    EX_STR_PATTERN_HAS_INVALID_VALUE,
    EX_STR_WHITE_SPACE_HAS_INVALID_VALUE,
    EX_STR_MISSING_NAME_ATTRIBUTE,
    EX_STR_UNKNOWN,
    EX_STR_LAST_ENTRY
};

const char pExceptionStringArray[EX_STR_LAST_ENTRY][MAX_EXCEPTION_STRING_LENGTH] = { /*** ALWAYS ADD TO THE END OF THE ARRAY!!! ***/
                                                                                     "can not open xsd file", //
                                                                                     "simple type already defined",
                                                                                     "complex type already defined",
                                                                                     "attribute group already defined",
                                                                                     "can not open/parse environment xml configuration",
                                                                                     "xpath does not exist in supplied tree",
                                                                                     "the xml file is missing a required attribute based on the xsd",
                                                                                     "length type can not have an empty value attribute",
                                                                                     "length value must be greater than or equal to zero",
                                                                                     /*** ADD CORRESPONDING ENTRY TO pExceptionStringActionArray ***/
                                                                                    };

const char pExceptionStringActionArray[EX_STR_LAST_ENTRY*2][MAX_EXCEPTION_STRING_LENGTH] = {  /*** ALWAYS ADD TO THE END OF THE ARRAY!!! ***/
                                                                                            /* 1 */ "Ensure that input xsd files exist and that it's permissions are set properly",
                                                                                            /* 2 */ "Multiple xs:simpleType tags with the same name defined in xsd files. Try processing xsd files using -use parameter and only specify 1 xsd file for processing." ,
                                                                                            /* 3 */ "Multiple xs:complexType tags with the same name defined in xsd files. Try processing xsd files using -use parameter and only specify 1 xsd file for processing.",
                                                                                            /* 4 */ "Multiple xs:attributeGroup tags with the same name defined in xsd files. Try processing xsd files using -use parameter and only specify 1 xsd file for processing.",
                                                                                            /* 5 */ "Failed to open/parss specified configuration file.  Verify file exits, permissions are set properly, and the file is valid.",
                                                                                            /* 6 */ "The XML file may have errors.",
                                                                                            /* 7 */ "The XML file may have errors.  An attribute marked as required in the XSD is missing in the xml file."
                                                                                            /* 8 */ "The XSD has an node  xs:restriction type with an xs:length datatype that has not value; value is required",
                                                                                            /* 9 */ "The XSD has an node xs:length @value is not greater than or equal to 0",
                                                                                            /* 10 */ "The XSD has an node xs:fractionDigits that has a value that is not greater than or equl to 0",
                                                                                            /* 11 */ "The XSD has an node xs:minLength value that is not greater than or equl to 0",
                                                                                            /* 12 */ "The XSD has an node xs:minInclusive that has no value attribute",
                                                                                            /* 13 */ "The XSD has an node xs:max:Exclusive that has no value attribute",
                                                                                            /* 14 */ "The XSD has an node xs:max:Inclusive that has no value attribute",
                                                                                            /* 15 */ "The XSD has a node xs:maxLength @value is not greater than or equal to 0",
                                                                                            /* 16 */ "The XSD has a node xs:pattern @value is not set",
                                                                                            /* 17 */ "The XSD has a node xs:totalDigits @value is not greater than or equal to 0",
                                                                                            /* 17 */ "The XSD has a node xs:whitespace @value is not valid",
                                                                                            /*** ADD CORRESPONDING ENTRY TO pExceptionStringActionArray ***/
                                                                                        };

enum eActionArray { EACTION_FRACTION_DIGITS_HAS_BAD_LENGTH = 10,
                    EACTION_MIN_LENGTH_BAD_LENGTH = 11,
                    EACTION_MIN_INCLUSIVE_NO_VALUE = 12,
                    EACTION_MAX_EXCLUSIVE_NO_VALUE = 13,
                    EACTION_MAX_INCLUSIVE_NO_VALUE = 14,
                    EACTION_MAX_LENGTH_BAD_LENGTH = 15,
                    EACTION_PATTERN_MISSING_VALUE = 16,
                    EACTION_TOTAL_DIGITS_BAD_LENGTH = 17,
                    EACTION_WHITE_SPACE_BAD_VALUE = 18
                  };

IException *MakeExceptionFromMap(int code, enum eExceptionCodes, const char* pMsg = NULL);
IException *MakeExceptionFromMap(enum eExceptionCodes, const char* pMsg = NULL);

#endif // _EXCEPTION_STRINGS_HPP_
