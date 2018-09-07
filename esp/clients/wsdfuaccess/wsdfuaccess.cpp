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

#include "jliball.hpp"
#include "dautils.hpp"
#include "seclib.hpp"
#include "ws_dfu.hpp"

#include "wsdfuaccess.hpp"

namespace wsdfuaccess
{

CSecAccessType translateToCSecAccessAccessType(SecAccessFlags from)
{
    switch (from)
    {
        case SecAccess_Access:
            return CSecAccessType_Access;
        case SecAccess_Read:
            return CSecAccessType_Read;
        case SecAccess_Write:
            return CSecAccessType_Write;
        case SecAccess_Full:
            return CSecAccessType_Full;
        case SecAccess_None:
        default:
            return CSecAccessType_None;
    }
}

bool getFileAccess(StringBuffer &metaInfo, const char *serviceUrl, const char *jobId, const char *logicalName, SecAccessFlags access, unsigned expirySecs, const char *user, const char *token)
{
    Owned<IClientWsDfu> dfuClient = createWsDfuClient();
    dfuClient->addServiceUrl(serviceUrl);
    dfuClient->setUsernameToken(user, token, "");

    Owned<IClientDFUFileAccessRequest> dfuReq = dfuClient->createDFUFileAccessRequest();

    CDfsLogicalFileName lfn;
    lfn.set(logicalName);

    StringBuffer cluster, lfnName;
    lfn.getCluster(cluster);
    lfn.get(lfnName); // remove cluster if present

    dfuReq->setName(lfnName);
    dfuReq->setCluster(cluster);
    dfuReq->setExpirySeconds(expirySecs);
    dfuReq->setAccessRole(CFileAccessRole_Engine);
    dfuReq->setAccessType(translateToCSecAccessAccessType(access));
    dfuReq->setJobId(jobId);

    Owned<IClientDFUFileAccessResponse> dfuResp = dfuClient->DFUFileAccess(dfuReq);

    const IMultiException *excep = &dfuResp->getExceptions(); // NB: warning despite getXX name, this does not Link
    if (excep->ordinality() > 0)
        throw LINK((IMultiException *)excep); // JCSMORE - const IException.. not caught in general..

    metaInfo.append(dfuResp->getMetaInfoBlob());
    return true;
}

} // namespace wsdfuaccess
