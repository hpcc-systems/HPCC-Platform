#include "jlib.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "gpgcodesigner.hpp"

class GpgCodeSigner : implements ICodeSigner
{
    virtual bool sign(const char * text, const char * userId, const char * passphrase, StringBuffer & errmsg, StringBuffer & signedText) override;
    virtual bool verifySignature(const char * text, StringBuffer & signer, StringBuffer & errmsg) override;
    virtual bool hasSignature(const char * text) const override;
    virtual StringBuffer &stripSignature(const char * text, StringBuffer & unsignedText) const override;
    virtual StringArray &getUserIds(StringArray & userIds, StringBuffer & errmsg) override;
private:
    void initGpg(void);
    bool getKeyGrip(const char * user, StringBuffer & keygrip);
    void clearPassphrase(const char * key);
    StringBuffer & sanitizeUserId(const char * userId, StringBuffer & sanitizedUserId);
    StringBuffer & sanitizePassphrase(const char * passphrase, StringBuffer & sanitizedPassphrase);
    bool isGpgV1 = false;
    bool isGpgInstalled = false;
    static constexpr const char* signatureMsgHeader = "-----BEGIN PGP SIGNED MESSAGE-----";
    static constexpr const char* signatureBegin = "-----BEGIN PGP SIGNATURE-----";
    CriticalSection crit;
};

GpgCodeSigner gpgCodeSigner;

extern jlib_decl ICodeSigner &queryGpgCodeSigner()
{
    return gpgCodeSigner;
}

void GpgCodeSigner::initGpg(void)
{
    if (isGpgInstalled) return;

    CriticalBlock block(crit);
    StringBuffer cmd, output, errmsg;
    int ret = runExternalCommand(output, errmsg, "gpg --version", nullptr);
    if (ret != 0)
        throw MakeStringException(-1, "Error running gpg: %s", errmsg.str());
    isGpgV1 = strstr(output.str(), "gpg (GnuPG) 1.");
    isGpgInstalled = true;
}

bool GpgCodeSigner::sign(const char *text, const char *userId, const char *passphrase, StringBuffer &signedText, StringBuffer &errmsg)
{
    initGpg();

    StringBuffer keygrip;
    if (!isGpgV1 && !getKeyGrip(userId, keygrip))
    {
        errmsg.set("Key not found");
        clearPassphrase(keygrip);
        return false;
    }
    
    StringBuffer cmd, sanitizedUserId, sanitizedPassphrase;
    sanitizeUserId(userId, sanitizedUserId);
    sanitizePassphrase(passphrase, sanitizedPassphrase);
    cmd.setf("gpg --clearsign -u \"%s\" --yes --batch --passphrase-fd 0", sanitizedUserId.str());
    if (!isGpgV1)
        cmd.append(" --pinentry-mode loopback");
    VStringBuffer input("%s\n", sanitizedPassphrase.str());
    input.append(text);

    int ret = runExternalCommand(signedText, errmsg, cmd.str(), input.str());
    if (ret != 0 || signedText.length() == 0)
    {
        errmsg.set("Code sign failed");
        UERRLOG("gpg clearsign error: [%d] %s\nOutput: n%s", ret, errmsg.str(), signedText.str());
        return false;
    }

    if (!isGpgV1 && keygrip.length() > 0)
        clearPassphrase(keygrip.str());

    return true;
}

bool GpgCodeSigner::verifySignature(const char *text, StringBuffer &signer, StringBuffer &errmsg)
{
    initGpg();
    Owned<IPipeProcess> pipe = createPipeProcess();
    if (!pipe->run("gpg", "gpg --verify -", ".", true, false, true, 0, false))
        throw makeStringException(0, "Signature could not be checked because gpg was not found");
    pipe->write(strlen(text), text);
    pipe->closeInput();
    unsigned retcode = pipe->wait();

    StringBuffer buf;
    Owned<ISimpleReadStream> pipeReader = pipe->getErrorStream();
    readSimpleStream(buf, *pipeReader);
    DBGLOG("GPG %d %s", retcode, buf.str());
    if (retcode)
    {
        errmsg.setf("gpg verify failed: %s (%d)", buf.str(), retcode);
    }
    else
    {
        const char * sigprefix = "Good signature from \"";
        const char * const s = buf.str();
        const char * match = strstr(s, sigprefix);
        if (match)
        {
            match += strlen(sigprefix);
            const char * const end = strchr(match, '\"');
            if (end)
            {
                signer.append(end-match, match);
                return true;
            }
        }
    }
    return false;
}

bool GpgCodeSigner::hasSignature(const char *text) const
{
    return startsWith(text, signatureMsgHeader);
}

StringBuffer &GpgCodeSigner::stripSignature(const char *text, StringBuffer &unsignedText) const
{
    if (!hasSignature(text))  // no signature -> return unchanged
        return unsignedText.set(text);

    const char *head = text;
    head += strlen(signatureMsgHeader);     // skip header
    while ((head = strchr(head, '\n')) != nullptr)
    {
        head++;
        if (*head=='\n')
        {
            head++;
            break;
        }
        else if (*head=='\r' && head[1]=='\n')
        {
            head += 2;
            break;
        }
    }

    if (!head)
        return unsignedText.set(text);

    const char *tail = strstr(head, signatureBegin);
    if (!tail)
        return unsignedText.set(text);
    return unsignedText.append(tail-head, head);
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

StringArray &GpgCodeSigner::getUserIds(StringArray &userIds, StringBuffer &errmsg)
{
    initGpg();
    StringBuffer output("\n");
    errmsg.clear();
    int ret = runExternalCommand(output, errmsg, "gpg --list-secret-keys --with-colon", nullptr);
    if (ret != 0)
        throw MakeStringException(-1, "Error running gpg: %s", errmsg.str());

    const char* START = "\nuid:";
    if (isGpgV1)
        START = "\nsec:";
    int startlen = strlen(START);
    const int SKIP = 8;
    const char* line = output.str();
    StringArray uids;
    while (line && *line)
    {
        line = strstr(line, START);
        if (!line)
            break;
        line += startlen;
        line = skipn(line, ':', SKIP);
        if (!*line)
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
    for (unsigned i = 0; i < uids.length(); i++)
    {
        if (strcmp(uids.item(i), current) != 0)
        {
            current = uids.item(i);
            userIds.append(current);
        }
    }

    return userIds;
}

bool GpgCodeSigner::getKeyGrip(const char *userId, StringBuffer &keygrip)
{
    initGpg();
    keygrip.clear();

    StringBuffer cmd;
    if (isGpgV1)
        cmd.appendf("gpg --list-secret-keys \"=%s\"", userId); // = means exact match
    else
        cmd.appendf("gpg --list-secret-keys --with-keygrip \"=%s\"", userId); // = means exact match

    StringBuffer output, errmsg;
    int ret = runExternalCommand(output, errmsg, cmd.str(), nullptr);
    if (ret != 0 || strstr(output.str(), userId) == nullptr)
        return false;
    auto kgptr = strstr(output.str(), "Keygrip = ");
    if (kgptr)
        keygrip.append(40, kgptr+10);
    else
        return false;
    return true;
}

void GpgCodeSigner::clearPassphrase(const char* key)
{
    initGpg();
    StringBuffer output, errmsg;
    VStringBuffer cmd("gpg-connect-agent \"clear_passphrase --mode=normal %s\" /bye", key);
    runExternalCommand(output, errmsg, cmd.str(), nullptr);
}

StringBuffer & GpgCodeSigner::sanitizeUserId(const char * userId, StringBuffer & sanitizedUserId)
{
    sanitizedUserId.clear();
    sanitizedUserId.ensureCapacity(strlen(userId));
    const char *p = userId;
    while (*p && *p==' ') ++p;
    while (*p)
    {
        if (isalnum(*p) || *p==' ' || *p=='.' || *p=='@' || *p =='<' || *p=='>')
            sanitizedUserId.append(*p);
        ++p;
    };
    return sanitizedUserId;
}

StringBuffer & GpgCodeSigner::sanitizePassphrase(const char * passphrase, StringBuffer & sanitizedPassphrase)
{
    sanitizedPassphrase.clear();
    sanitizedPassphrase.ensureCapacity(strlen(passphrase));
    const char *p = passphrase;
    // Note: strictly speaking any characters should be valid for passphrases. However, fully
    // open passphrase opens the possibility of command injection.  The characters accepted
    // should be sufficient to choose a strong passphrase.
    while (*p)
    {
        if (isalnum(*p) || *p==' ' || *p=='.' || *p=='@' || *p =='$' || *p=='-' || *p=='_' 
              || *p=='$' || *p=='(' || *p==')' || *p=='#' || *p=='!' || *p=='%' || *p=='/')
            sanitizedPassphrase.append(*p);
        ++p;
    };
    return sanitizedPassphrase;
}
