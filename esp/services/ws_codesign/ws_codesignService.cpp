/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

#include "ws_codesignService.hpp"
#include "jutil.hpp"
#include "codesigner.hpp"

Cws_codesignEx::Cws_codesignEx()
{
}

Cws_codesignEx::~Cws_codesignEx()
{
}

void Cws_codesignEx::init(IPropertyTree *cfg, const char *process, const char *service)
{
    if(cfg == nullptr)
        throw MakeStringException(-1, "Cannot initialize Cws_codesignEx, cfg is NULL");

    StringBuffer xpath;
    xpath.appendf("Software/EspProcess[@name=\"%s\"]/EspService[@name=\"%s\"]", process, service);
    m_serviceCfg.setown(cfg->getPropTree(xpath.str()));
}

bool Cws_codesignEx::onSign(IEspContext &context, IEspSignRequest &req, IEspSignResponse &resp)
{
    resp.setRetCode(-1);

    StringBuffer userid(req.getUserID());
    userid.trim();
    const char* text = req.getText();
    if (userid.length() == 0 || !text || !*text)
    {
        resp.setErrMsg("Please provide both UserID and Text");
        return false;
    }
    StringBuffer tmpbuf, signedText, errmsg;
    if (queryCodeSigner().hasSignature(text))
        text = queryCodeSigner().stripSignature(text, tmpbuf).str();  // remove  existing signature
    bool ret = queryCodeSigner().sign(text, userid.str(), req.getKeyPass(), signedText, errmsg);

    if (!ret)
    {
        resp.setErrMsg(errmsg.str());
        return false;
    }

    resp.setRetCode(0);
    resp.setSignedText(signedText.str());

    return true;
}

bool Cws_codesignEx::onListUserIDs(IEspContext &context, IEspListUserIDsRequest &req, IEspListUserIDsResponse &resp)
{
    StringArray userIds;
    StringBuffer errmsg;
    queryCodeSigner().getUserIds(userIds, errmsg);

    resp.setUserIDs(userIds);

    return true;
}
