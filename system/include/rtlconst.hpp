/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

#ifndef _RTLCONST_H_
#define _RTLCONST_H_

// This file contains system wide constants used by runtime and compile time components
// NB: Do not change the values here they must remain the same
// Add new types to the end

enum type_vals
{
    type_boolean        = 0,
    type_int            = 1,
    type_real           = 2,
    type_decimal        = 3,
    type_string         = 4,
    type_alias          = 5, // This is only used when serializing expression graphs
    type_date           = 6,
    type_swapfilepos    = 7,
    type_biasedswapint  = 8,
    type_bitfield       = 9,
    type_keyedint       = 10,
    type_char           = 11,
    type_enumerated     = 12,
    type_record         = 13,
    type_varstring      = 14,
    type_blob           = 15,
    type_data           = 16,
    type_pointer        = 17,
    type_class          = 18,
    type_array          = 19,
    type_table          = 20,
    type_set            = 21,
    type_row            = 22,
    type_groupedtable   = 23,
    type_void           = 24,
    type_alien          = 25,
    type_swapint        = 26,
    type_none           = 27,
    type_packedint      = 28,
    type_filepos        = 29,
    type_qstring        = 30,
    type_unicode        = 31,
    type_any            = 32,
    type_varunicode     = 33,
    type_pattern        = 34,
    type_rule           = 35,
    type_token          = 36,
    type_feature        = 37,
    type_event          = 38,
    type_null           = 39,       // not the same as type_void, which should be reserved for actions.
    type_scope          = 40,
    type_utf8           = 41,
    type_transform      = 42,
    type_ifblock        = 43,       // not a real type -but used for the rtlfield serialization
    type_function       = 44,
    type_sortlist       = 45,
    type_dictionary     = 46,

    type_max,

    type_modifier       = 0xff,     // used by getKind()
    type_unsigned       = 0x100,  // combined with some of the above, when returning summary type information. Not returned by getTypeCode()
    type_ebcdic         = 0x200,   // combined with some of the above, when returning summary type information. Not returned by getTypeCode()

//Some pseudo types - never actually created
    type_stringorunicode= 0xfc, // any string/unicode variant
    type_numeric        = 0xfd,
    type_scalar         = 0xfe,
};

#endif
