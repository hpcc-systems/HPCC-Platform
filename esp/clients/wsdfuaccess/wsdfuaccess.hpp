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

#include "dafsstream.hpp"

class StringBuffer;
interface IUserDescriptor;
interface IFileDescriptor;
interface IConstEnvironment;

using dafsstream::IDFUFileAccess;
using dafsstream::DFUFileType;
namespace wsdfuaccess
{

WSDFUACCESS_API IDFUFileAccess *lookupDFUFile(const char *logicalName, const char *requestId, unsigned expirySecs, const char *user, const char *password);
WSDFUACCESS_API IDFUFileAccess *lookupDFUFile(const char *logicalName, const char *requestId, unsigned expirySecs, IUserDescriptor *userDesc);

WSDFUACCESS_API IDFUFileAccess *createDFUFile(const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, bool compressed, const char *user, const char *password);
WSDFUACCESS_API IDFUFileAccess *createDFUFile(const char *logicalName, const char *cluster, DFUFileType type, const char *recDef, const char *requestId, unsigned expirySecs, bool compressed, IUserDescriptor *userDesc);

WSDFUACCESS_API void publishDFUFile(IDFUFileAccess *dfuFile, bool overwrite, IUserDescriptor *userDesc);
WSDFUACCESS_API void publishDFUFile(IDFUFileAccess *dfuFile, bool overwrite, const char *user, const char *password);

/* createDFUFileAccess() and encodeDFUFileMeta() will normally be called by the DFU service
 * via a DFS file request. So that the meta info blob can be returned to the client of the service.
 * However, for testing purposes it's also useful to create these blobs elsewhere directly from IFileDescriptor's
 */
WSDFUACCESS_API IPropertyTree *createDFUFileMetaInfo(const char *fileName, IFileDescriptor *fileDesc, const char *requestId, const char *accessType, unsigned expirySecs,
                                                     IUserDescriptor *userDesc, const char *keyPairName, unsigned port, bool secure, unsigned maxFileAccessExpirySeconds);
WSDFUACCESS_API StringBuffer &encodeDFUFileMeta(StringBuffer &metaInfoBlob, IPropertyTree *metaInfo, IConstEnvironment *environment);


} // end of namespace wsdfuaccess

#endif // _WSDFUACCESS_HPP
