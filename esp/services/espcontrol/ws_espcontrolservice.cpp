/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2015 HPCC Systems.

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

#ifdef _USE_OPENLDAP
#include "ldapsecurity.ipp"
#endif

#include "ws_espcontrolservice.hpp"
#include "jlib.hpp"
#include "exception_util.hpp"

bool CWSESPControlEx::onSetLogging(IEspContext& context, IEspSetLoggingRequest& req, IEspSetLoggingResponse& resp)
{
    try
    {
#ifdef _USE_OPENLDAP
        CLdapSecManager* secmgr = dynamic_cast<CLdapSecManager*>(context.querySecManager());
        if(secmgr && !secmgr->isSuperUser(context.queryUser()))
            throw MakeStringException(ECLWATCH_SUPER_USER_ACCESS_DENIED, "Failed to change log settings. Permission denied.");
#endif

        if (!m_container)
            throw MakeStringException(ECLWATCH_INTERNAL_ERROR, "Failed to access container.");

        if (!req.getLoggingLevel_isNull())
            m_container->setLogLevel(req.getLoggingLevel());
        if (!req.getLogRequests_isNull())
            m_container->setLogRequests(req.getLogRequests());
        if (!req.getLogResponses_isNull())
            m_container->setLogResponses(req.getLogResponses());
        resp.setStatus(0);
        resp.setMessage("Logging settings are updated.");
    }
    catch(IException* e)
    {
        FORWARDEXCEPTION(context, e, ECLWATCH_INTERNAL_ERROR);
    }
    return true;
}
