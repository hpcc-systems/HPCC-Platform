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

#ifndef _THKJCOMMON_HPP_
#define _THKJCOMMON_HPP_

#ifdef ACTIVITYSLAVES_EXPORTS
 #define activityslaves_decl DECL_EXPORT
#else
 #define activityslaves_decl DECL_IMPORT
#endif

class CJoinGroup;
struct KeyLookupHeader
{
    CJoinGroup *jg;
};
struct FetchRequestHeader
{
    offset_t fpos;
    CJoinGroup *jg;
    unsigned __int64 sequence;
};
struct FetchReplyHeader
{
    static const unsigned __int64 fetchMatchedMask = 0x8000000000000000;
    unsigned __int64 sequence; // fetchMatchedMask used to screen top-bit to denote whether reply fetch matched or not
};
template <class HeaderStruct>
void getHeaderFromRow(const void *row, HeaderStruct &header)
{
    memcpy(&header, row, sizeof(HeaderStruct));
}
enum GroupFlags:unsigned { gf_null=0x0, gf_limitatmost=0x01, gf_limitabort=0x02, gf_start=0x04, gf_head=0x08 };
enum KJServiceCmds:byte { kjs_nop, kjs_keyopen, kjs_keyread, kjs_keyclose, kjs_fetchopen, kjs_fetchread, kjs_fetchclose };
enum KJFetchFlags:byte { kjf_nop=0x0, kjf_compressed=0x1, kjf_encrypted=0x2 };
enum KJServiceErrorCode:byte { kjse_nop, kjse_exception, kjse_unknownhandle };

constexpr unsigned partBits = 24;
constexpr unsigned partMask = 0x00ffffff;
// the same as part, but for clarify has own symbols
constexpr unsigned slaveBits = 24;
constexpr unsigned slaveMask = 0x00ffffff;


#endif
