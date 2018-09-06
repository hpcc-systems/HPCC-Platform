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
#include "dadfs.hpp"
#include "environment.hpp"

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

class CDFSFileAccessHook : public CSimpleInterfaceOf<IDistributeFileAccessHook>
{
    mutable Owned<IEnvironmentFactory> factory;
    mutable Owned<IConstEnvironment> env; // for fileAccessUrl
    unsigned expirySecs;

    StringBuffer &getTokenValue(StringBuffer &res, const char *key, const char *tokens) const
    {
        size32_t keyLen = strlen(key);
        const char *cur = tokens;
        while (true)
        {
            const char *tokenStart = cur;
            while (true)
            {
                if ('=' == *cur)
                    break;
                else if ('\0' == *cur) // implies malformed
                    return res; // nothing returned
                ++cur;
            }
            size32_t tokenKeyLen = cur-tokenStart;
            if (0 == tokenKeyLen) // implied malformed
                return res; // nothing returned
            ++cur; // skip '='
            const char *valueStart = cur;
            while (true)
            {
                if (('\0' == *cur) || (',' == *cur))
                    break;
                else if ('=' == *cur) // implies malformed
                    return res; // nothing returned
                ++cur;
            }
            size32_t valueLen = cur - valueStart;
            if (0 == valueLen) // implied malformed
                return res; // nothing returned
            if ((keyLen == tokenKeyLen) && (0 == memcmp(tokenStart, key, keyLen)))
                return res.append(valueLen, valueStart);
            if ('\0' == *cur)
                return res; // nothing returned
            ++cur; // skip ','
        }
    }
    StringBuffer &getForeignFileAccessUrl(StringBuffer &fileAccessUrl, const CDfsLogicalFileName &lfn) const
    {
        // JCSMORE
        return fileAccessUrl;
    }
public:
    CDFSFileAccessHook(unsigned _expirySecs) : expirySecs(_expirySecs)
    {
    }
    // IDistributeFileAccessHook impl.
    virtual bool lookup(StringBuffer &fileMetaInfo, CDfsLogicalFileName &lfn, IUserDescriptor *user, bool write) const override
    {
        /* JCSMORE - what this SHOULD do, is get all the meta required to build a IDistributedFile,
         * but that will require a lot of refactoring.
         */

        if (!factory)
        {
            factory.setown(getEnvironmentFactory(true));
            env.setown(factory->openEnvironment());
        }
        if (write)
            return true; // because server side is unimplemented for write at the moment, so no point.
        try
        {
            StringBuffer fileAccessUrl;
            if (lfn.isForeign())
            {
                if (0 == getForeignFileAccessUrl(fileAccessUrl, lfn).length()) // lookup file access url
                    return true;
            }
            else
                fileAccessUrl.append(env->getFileAccessUrl());
            if (isEmptyString(fileAccessUrl)) // not configured for secure access, just check scope
                return checkLogicalName(lfn, user, true, write, true, nullptr);

            StringBuffer wuidValue;
            getTokenValue(wuidValue, "jobid", user->queryExtra());
            assertex(wuidValue.length());
            StringBuffer userName;
            user->getUserName(userName);
            SecAccessFlags access = write ? SecAccess_Write : SecAccess_Read;
            return getFileAccess(fileMetaInfo, fileAccessUrl, wuidValue.str(), lfn.get(), access, expirySecs, userName, nullptr);
        }
        catch (IException *e)
        {
            StringBuffer eMsg;
            e->errorMessage(eMsg);
            int code = e->errorCode();
            e->Release();
            throwStringExceptionV(0, "Security failure looking up file: %s - [%d, %s]", lfn.get(), code, eMsg.str());
        }
        return false;
    }
};

IDistributeFileAccessHook *createDFSFileAccessHook(unsigned expirySecs)
{
    return new CDFSFileAccessHook(expirySecs);
}

} // namespace wsdfuaccess
