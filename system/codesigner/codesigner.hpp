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

#ifndef CODESIGN_HPP
#define CODESIGN_HPP

#include "jlib.hpp"

interface ICodeSigner
{
    virtual bool sign(const char *text, const char *user_id, const char *passphrase, StringBuffer &signedText, StringBuffer &errmsg) = 0;
    virtual bool verifySignature(const char *text, StringBuffer &signer, StringBuffer &errmsg) = 0;
    virtual bool hasSignature(const char *text) const = 0;
    virtual StringBuffer &stripSignature(const char *text, StringBuffer &unsignedText) const = 0;
    virtual StringArray &getUserIds(StringArray &userIds, StringBuffer &errmsg) = 0;
};

extern jlib_decl ICodeSigner &queryCodeSigner();

#endif
