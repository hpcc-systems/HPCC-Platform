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

void Cws_codesignEx::clearPassphrase(const char* key)
{
    StringBuffer output, errmsg;
    VStringBuffer cmd("gpg-connect-agent \"clear_passphrase --mode=normal %s\" /bye", key);
    runExternalCommand(output, errmsg, cmd.str(), nullptr);
}

bool Cws_codesignEx::onSign(IEspContext &context, IEspSignRequest &req, IEspSignResponse &resp)
{
    resp.setRetCode(-1);

    StringBuffer keyid(req.getKeyIdentifier());
    keyid.trim();
    const char* text = req.getText();
    if (keyid.length() == 0 || !text || !*text)
    {
        resp.setErrMsg("Please provide both KeyIdentifier and Text");
        return false;
    }
    if (strstr(keyid.str(), "\""))
    {
        resp.setErrMsg("Invalid KeyIdentifier");
        return false;
    }

    StringBuffer cmd, output, errmsg;

    int ret = runExternalCommand(output, errmsg, "gpg --version", nullptr);
    if (ret != 0)
        throw MakeStringException(-1, "Error running gpg: %s", errmsg.str());
    bool isGPGv1 = strstr(output.str(), "gpg (GnuPG) 1.");

    output.clear();
    errmsg.clear();
    if (isGPGv1)
        cmd.appendf("gpg --list-secret-keys \"%s\"", keyid.str());
    else
        cmd.appendf("gpg --list-secret-keys --with-keygrip \"%s\"", keyid.str());
    ret = runExternalCommand(output, errmsg, cmd.str(), nullptr);
    if (ret != 0 || strstr(output.str(), keyid.str()) == nullptr)
    {
        resp.setErrMsg("Key not found");
        return false;
    }

    StringBuffer keygrip;
    if (!isGPGv1)
    {
        auto kgptr = strstr(output.str(), "Keygrip = ");
        if (kgptr)
            keygrip.append(40, kgptr+10);

        if (keygrip.length() > 0)
            clearPassphrase(keygrip.str());
    }

    output.clear();
    errmsg.clear();
    cmd.clear().appendf("gpg --clearsign -u \"%s\" --yes --batch --passphrase-fd 0", keyid.str());
    if (!isGPGv1)
        cmd.append(" --pinentry-mode loopback");
    VStringBuffer input("%s\n", req.getKeyPass());
    input.append(text);
    ret = runExternalCommand(output, errmsg, cmd.str(), input.str());
    if (ret != 0 || output.length() == 0)
    {
        UERRLOG("gpg clearsign error: [%d] %s\nOutput: n%s", ret, errmsg.str(), output.str());
        resp.setErrMsg("Failed to sign text, please check service log for details");
        return false;
    }

    resp.setRetCode(0);
    resp.setSignedText(output.str());

    if (!isGPGv1 && keygrip.length() > 0)
        clearPassphrase(keygrip.str());

    return true;
}
