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

    StringBuffer userid(req.getUserID());
    userid.trim();
    const char* text = req.getText();
    if (userid.length() == 0 || !text || !*text)
    {
        resp.setErrMsg("Please provide both UserID and Text");
        return false;
    }

    if (strstr(userid.str(), "\""))
    {
        resp.setErrMsg("Invalid UserID");
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
        cmd.appendf("gpg --list-secret-keys \"=%s\"", userid.str()); // = means exact match
    else
        cmd.appendf("gpg --list-secret-keys --with-keygrip \"=%s\"", userid.str()); // = means exact match
    ret = runExternalCommand(output, errmsg, cmd.str(), nullptr);
    if (ret != 0 || strstr(output.str(), userid.str()) == nullptr)
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
    cmd.clear().appendf("gpg --clearsign -u \"%s\" --yes --batch --passphrase-fd 0", userid.str());
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

const char* skipn(const char* str, char c, int n)
{
    for (int i = 0; i < n && str && *str; i++)
    {
        str = strchr(str, c);
        if (!str)
            break;
        str++;
    }
    return str;
}

bool Cws_codesignEx::onListUserIDs(IEspContext &context, IEspListUserIDsRequest &req, IEspListUserIDsResponse &resp)
{
    StringBuffer output, errmsg;

    int ret = runExternalCommand(output, errmsg, "gpg --version", nullptr);
    if (ret != 0)
        throw MakeStringException(-1, "Error running gpg: %s", errmsg.str());
    bool isGPGv1 = strstr(output.str(), "gpg (GnuPG) 1.");

    const char* START = "\nuid:";
    if (isGPGv1)
        START = "\nsec:";
    int startlen = strlen(START);
    const int SKIP = 8;
    output.clear().append("\n");
    errmsg.clear();
    ret = runExternalCommand(output, errmsg, "gpg --list-secret-keys --with-colon", nullptr);
    if (ret != 0)
        throw MakeStringException(-1, "Error running gpg: %s", errmsg.str());
    const char* line = output.str();
    StringArray uids;
    while (line && *line)
    {
        line = strstr(line, START);
        if (!line)
            break;
        line += startlen;
        line = skipn(line, ':', SKIP);
        if (!line || !*line)
            break;
        const char* uid_s = line;
        while (*line != '\0' && *line != ':')
            line++;
        if (line > uid_s)
        {
            StringBuffer uid(line - uid_s, uid_s);
            uid.trim();
            if (uid.length() > 0)
                uids.append(uid.str());
        }
    }
    uids.sortAscii(false);
    const char* current = "";
    StringArray& respuserids = resp.getUserIDs();
    for (int i = 0; i < uids.length(); i++)
    {
        if (strcmp(uids.item(i), current) != 0)
        {
            current = uids.item(i);
            respuserids.append(current);
        }
    }
    return true;
}
