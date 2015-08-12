/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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
#pragma warning (disable : 4786)

#include "esdl_svc_engine.hpp"
#include "params2xml.hpp"

void CEsdlSvcEngine::init(IPropertyTree *cfg, const char *process, const char *service)
{
    EsdlServiceImpl::init(cfg, process, service);

    m_service_ctx.setown( createPTree("Context", false) );
    ensurePTree(m_service_ctx, "Row");
}

CEsdlSvcEngine::~CEsdlSvcEngine()
{
}

CEsdlSvcEngineSoapBindingEx::CEsdlSvcEngineSoapBindingEx()
{
}

CEsdlSvcEngineSoapBindingEx::CEsdlSvcEngineSoapBindingEx(IPropertyTree* cfg, const char *bindname, const char *procname) : EsdlBindingImpl(cfg, bindname, procname)
{
}

void CEsdlSvcEngineSoapBindingEx::handleSoapRequestException(IException *e, const char *source)
{
    WsErrorType  errorType = WSERR_NOERR;

    const int    errorcode = e->errorCode();
    StringBuffer errorMessage;
    e->errorMessage(errorMessage);

    ESPLOG(LogMax,"EsdlBindingImpl:HandleSoapRequest response error code: %d /nmerror message: %s",  errorcode, errorMessage.str());

    IWsException *wsex = dynamic_cast<IWsException *> (e);

    switch (errorcode){
    case ERR_ESDL_BINDING_AUTHERROR: case ERR_ESDL_BINDING_BADACCOUNT: case ERR_ESDL_BINDING_BADABI:
    case ERR_ESDL_BINDING_BADBRAND:  case ERR_ESDL_BINDING_BADBROKER:  case ERR_ESDL_BINDING_TESTCASE:
        errorType = WSERR_CLIENT ;
        break;
    case 2:
        e->Release();
        wsex = makeWsException(ERR_ESDL_BINDING_BADREQUEST, WSERR_CLIENT, source, errorMessage.str());
        break;
    default:
        errorType = WSERR_SERVER;
        break;
    }

    if ( wsex )
        throw wsex;
    else
        throw makeWsException( *e, errorType, source );
}

IPropertyTree *CEsdlSvcEngine::createTargetContext(IEspContext &context, IPropertyTree *tgtcfg, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *req_pt)
{
    const char *querytype = tgtcfg->queryProp("@querytype");
    if (!querytype || !strieq(querytype, "ROXIE")) //only roxie?
        return NULL;

    StringBuffer trxid;
    generateTransactionId(context, trxid);

    Owned<IPropertyTree> localCtx(createPTreeFromIPT(m_service_ctx, ipt_none));
    ensurePTree(localCtx, "Row/Common");
    localCtx->setProp("Row/Common/TransactionId", trxid.str());

    return localCtx.getLink();
}

void CEsdlSvcEngine::generateTransactionId(IEspContext & context, StringBuffer & trxid)
{
    //RANDOMNUM_DATE for now.
    CriticalBlock b(trxIdCritSec);
    Owned<IJlibDateTime> _timeNow =  createDateTimeNow();
    SCMStringBuffer _dateString;
    _timeNow->getDateString(_dateString);
    trxid.appendf("%u_%s",getRandom(),_dateString.str());
}

void CEsdlSvcEngine::esdl_log(IEspContext &context, IEsdlDefService &srvdef, IEsdlDefMethod &mthdef, IPropertyTree *tgtcfg, IPropertyTree *tgtctx, IPropertyTree *req_pt, const char *rawresp, const char *logdata, unsigned int timetaken)
{
}
