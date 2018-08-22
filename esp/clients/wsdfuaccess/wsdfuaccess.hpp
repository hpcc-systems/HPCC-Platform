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

#ifndef _WSDFUACCESS_HPP
#define _WSDFUACCESS_HPP

#ifndef WSDFUACCESS_API

#ifdef WSDFUACCESS_EXPORTS
#define WSDFUACCESS_API DECL_EXPORT
#else
#define WSDFUACCESS_API DECL_IMPORT
#endif

#endif


class StringBuffer;

namespace wsdfuaccess
{

WSDFUACCESS_API bool getFileAccess(StringBuffer &metaInfo, const char *serviceUrl, const char *jobId, const char *logicalName, SecAccessFlags access, unsigned expirySecs, const char *user, const char *token);

} // end of namespace wsdfuaccess

#endif // _WSDFUACCESS_HPP
