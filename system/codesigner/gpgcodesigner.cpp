/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

#include "jlib.hpp"
#include "jexcept.hpp"
#include "jlog.hpp"
#include "jfile.hpp"
#include "jsecrets.hpp"
#include "gpgcodesigner.hpp"
#include "atomic"

/**
 * Encapsulate the gpg operations used for code signing
 * 
 * Note:
 * - One global instance of this class is sufficient
 * - the member functions in class is thread safe
 * - there are no special requirements for this objects destructions
 */
class GpgCodeSigner : implements ICodeSigner
{
    virtual void initForContainer();
    virtual void sign(const char * text, const char * userId, const char * passphrase, StringBuffer & signedText) override;
    virtual bool verifySignature(const char * text, StringBuffer & signer) override;
    virtual bool hasSignature(const char * text) const override;
    virtual StringBuffer &stripSignature(const char * text, StringBuffer & unsignedText) const override;
    virtual StringArray &getUserIds(StringArray & userIds) override;
private:
    void initGpg(void);
    bool importKey(const char *key, const char *passphrase);
    void importKeysFromSecret(const char * cat, const char *type);
    void importSigningKeysFromSecrets();
    void importVerifyKeysFromSecrets();
    bool getKeyGrip(const char * user, StringBuffer & keygrip);
    void clearPassphrase(const char * key);
    CriticalSection crit;
    std::atomic<bool> isGpgV1{false};
    std::atomic<bool> isInitialized{false};
    std::atomic<bool> getSignKeysFromSecrets {false};
    std::atomic<bool> getVerifyKeysFromSecrets {false};
    StringBuffer gpgOptions;
    StringBuffer gpgHomeDir;
    bool createNewGpgHomeDir = false;
    static constexpr const char* signatureMsgHeader = "-----BEGIN PGP SIGNED MESSAGE-----";
    static constexpr const char* signatureBegin = "-----BEGIN PGP SIGNATURE-----";
};

static GpgCodeSigner gpgCodeSigner;

extern jlib_decl ICodeSigner &queryGpgCodeSigner()
{
    return gpgCodeSigner;
}

/**
 * Initialize GpgCodeSigner
 * - confirm gpg is installed and working
 * - check gpg version
 */
void GpgCodeSigner::initGpg(void)
{
    if (isInitialized) return;
    CriticalBlock block(crit);
    if (isInitialized) return;

    StringBuffer cmd, output, errmsg;
    int ret = runExternalCommand(output, errmsg, "gpg --version", nullptr);
    if (ret != 0)
        throw makeStringExceptionV(MSGAUD_operator, CODESIGNER_ERR_GPG, "Error running gpg: %s", errmsg.str());
    isGpgV1 = strstr(output.str(), "gpg (GnuPG) 1.");
    if (createNewGpgHomeDir)
    {
        try
        {
            Owned<IFile> dir = createIFile(gpgHomeDir);
            dir->createDirectory();
            dir->setFilePermissions(0700);
        }
        catch (IException *e)
        {
            OERRLOG("Create directory failed: %s", gpgHomeDir.str());
            e->Release();
        }
        createNewGpgHomeDir = false;
    }
    isInitialized = true;
}

/**
 * Import a gpg key
 *
 * @param key           The gpg key
 * @param passphrase    The passphrase for key (optional)
 *
 * @return              True if successfully imported
 *                      False if import failed
 */
bool GpgCodeSigner::importKey(const char *key, const char *passphrase)
{
    initGpg();
    VStringBuffer cmd("gpg %s --batch --passphrase-fd 0 --import ", gpgOptions.str());
    StringBuffer output, errmsg;
    VStringBuffer input("%s\n", passphrase);
    input.append(key);
    int ret = runExternalCommand(output, errmsg, cmd.str(), input);
    if (ret != 0)
    {
        OERRLOG("External command failed: %s", errmsg.str());
        return false;
    }
    return true;
}

/**
 * Import a key from secrets
 *
 * @param key           Secrets category
 * @param keytype       Type of key: public or private
 */
void GpgCodeSigner::importKeysFromSecret(const char * cat, const char *keytype)
{
    unsigned importCount = 0;
    unsigned failCount = 0;
    for (int keyentry = 1; ; keyentry++)
    {
        VStringBuffer keysecretname("gpg-%s-key-%d", keytype, keyentry);
        Owned<IPropertyTree> secretKey = getSecret(cat, keysecretname.str());
        if (secretKey)
        {
            StringBuffer gpgKey;
            if (secretKey->getProp(keytype, gpgKey))
            {
                StringBuffer passphrase;
                secretKey->getProp("passphrase", passphrase);
                if (importKey(gpgKey, passphrase))
                    ++importCount;
                else
                    ++failCount;
            }
        }
        else
        {
            break; // finished importing keys
        }
    }
    if (failCount)
        OERRLOG("Keys imported from %s/gpg-%s-key-* failed: %u (succeeded %u)", cat, keytype, failCount, importCount);
    else
        DBGLOG("Keys imported from %s/gpg-%s-key-*: %u", cat, keytype, importCount);
}

/**
 * Imports signing keys from secrets
 * - Imports takes place just once. All subsequant calls ignored.
 * - (this may be called before every operation so keys imported only if needed)
 */
void GpgCodeSigner::importSigningKeysFromSecrets()
{
    if (!getSignKeysFromSecrets) return;
    CriticalBlock block(crit);
    if (!getSignKeysFromSecrets) return;
    importKeysFromSecret("codeSign","private");
    getSignKeysFromSecrets = false;
}

/**
 * Imports verifying keys from secrets
 * - Imports takes place just once. All subsequant calls ignored.
 * - (this may be called before every operation so keys imported only if needed)
 */
void GpgCodeSigner::importVerifyKeysFromSecrets()
{
    if (!getVerifyKeysFromSecrets) return;
    CriticalBlock block(crit);
    if (!getVerifyKeysFromSecrets) return;
    importKeysFromSecret("codeVerify","public");
    getVerifyKeysFromSecrets = false;
}

/**
 * Initialize gpg code signer for containers
 * - uses current directory for gpg instead of home directory (which may not exist)
 */
void GpgCodeSigner::initForContainer()
{
    // Processes running in containers may be owned by a user without a home directory
    // so use a directory under the current directory (create it later on 1st use)
    appendCurrentDirectory(gpgHomeDir, false);
    addPathSepChar(gpgHomeDir).append("gnugpg");
    gpgOptions.appendf(" --homedir %s", gpgHomeDir.str());
    createNewGpgHomeDir = true;
    getSignKeysFromSecrets = true;
    getVerifyKeysFromSecrets = true;
}

/**
 * Sign a block of text with a pgp signature - used of code signing
 *
 * @param text          Text block to sign
 * @param userId        The user id with which to sign the text - must match the user id in the keys
 * @param passphrase    The passphrase for the userId
 * @param signedText    Returned signed text which includes text block wrapped in armour and signature
 *
 * @return              Reference to signedText
 * 
 * Exceptions:
 * - CODESIGNER_ERR_BADUSERID - Invalid user id
 * - CODESIGNER_ERR_KEYNOTFOUND - User id not in key list
 * - CODESIGNER_ERR_SIGN - Signing failed: bad or missing passphrase
 */
void GpgCodeSigner::sign(const char * text, const char * userId, const char * passphrase, StringBuffer & signedText)
{
    initGpg();

    if (strchr(userId, '\"')!=nullptr || strlen(userId) > 2000)
        throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_BADUSERID, "Invalid user id: %s", userId);

    importSigningKeysFromSecrets();
    StringBuffer keygrip;
    if (!isGpgV1)
    {
        if (!getKeyGrip(userId, keygrip) || keygrip.length()==0)
            throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_KEYNOTFOUND, "Key for user not found: %s", userId);
        clearPassphrase(keygrip);
    }
    StringBuffer cmd, errmsg;
    cmd.setf("gpg %s --clearsign -u \"%s\" --yes --batch --passphrase-fd 0", gpgOptions.str(), userId);
    if (!isGpgV1)
        cmd.append(" --pinentry-mode loopback");
    VStringBuffer input("%s\n", passphrase);
    input.append(text);

    int ret = runExternalCommand(signedText, errmsg, cmd.str(), input.str());
    if (ret != 0 || signedText.length() == 0)
    {
        if (strstr(errmsg.str(),"No passphrase given")!=nullptr)
            errmsg.set("Passphrase required");
        else if (strstr(errmsg.str(),"Bad passphrase")!=nullptr)
            errmsg.set("Invalid passphrase");
        throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_SIGN, "Code sign failed: %s", errmsg.str());
    }

    if (!isGpgV1)
        clearPassphrase(keygrip.str());
}

/**
 * Check signature of signed block
 *
 * @param text          Block of signed text
 * @param signer        The user id of signer
 *
 * @return              True if valid signature
 *                      False if invalid signature
 * 
 * Exceptions:
 * - CODESIGNER_ERR_VERIFY - gpg verify could not be executed
 */
bool GpgCodeSigner::verifySignature(const char * text, StringBuffer & signer)
{
    initGpg();
    importVerifyKeysFromSecrets();

    Owned<IPipeProcess> pipe = createPipeProcess();
    VStringBuffer cmd("gpg %s --verify -", gpgOptions.str());
    if (!pipe->run("gpg", cmd.str(), ".", true, false, true, 0, false))
        throw makeStringException(MSGAUD_user, CODESIGNER_ERR_GPG, "Code sign verify failed (gpg --verify failed)");
    pipe->write(strlen(text), text);
    pipe->closeInput();
    unsigned retcode = pipe->wait();
    if (retcode && retcode!=1) // 1==gpg signature not verified.  Otherwise some error
        throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_VERIFY, "gpg verify failed - error %u", retcode);

    StringBuffer buf;
    Owned<ISimpleReadStream> pipeReader = pipe->getErrorStream();
    readSimpleStream(buf, *pipeReader);
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
    return false;
}

/**
 * Check if a text blockt has the header of a signed block
 *
 * @param text          Block of signed text/unsigned text
 *
 * @return              True if it has a signature header
 *                      False otherwise
 */
bool GpgCodeSigner::hasSignature(const char * text) const
{
    return startsWith(text, signatureMsgHeader);
}

/**
 * Remove text armour and signature from signed text block (if it exists)
 *
 * @param text          Block of signed text/unsigned text
 * @param unsignedText  Unsigned text block return 
 *
 * @return              Reference to unsignedText
 */
StringBuffer &GpgCodeSigner::stripSignature(const char * text, StringBuffer & unsignedText) const
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

/**
 * Skips the specified character a specified number of times
 *
 * @param str           input text
 * @param c             character to skip
 * @param n             number of matching characters to skip
 *
 * @return              Pointer to position in text after the specfied number of skips
 */
const char* skipn(const char * str, char c, int n)
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

/**
 * A list of the user ids
 *
 * @param userIds       Used to return the user ids
 *
 * @return              referenced to userIds
 * 
 * Exceptions:
 * - CODESIGNER_ERR_LISTKEYS - gpg list keys could not be executed
 */
StringArray &GpgCodeSigner::getUserIds(StringArray & userIds)
{
    initGpg();
    importSigningKeysFromSecrets();
    StringBuffer errmsg, output("\n");
    VStringBuffer cmd("gpg %s --list-secret-keys --with-colon", gpgOptions.str());
    int ret = runExternalCommand(output, errmsg, cmd.str(), nullptr);
    if (ret != 0)
    {
        IERRLOG("list secret keys failed: %s", errmsg.str());
        throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_LISTKEYS, "list secret keys failed: %s", errmsg.str());
    }

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

/**
 * A list of the user ids
 *
 * @param userId        User id to match
 * @param keygrip       Returned keygrip of match user
 *
 * @return              True if matching key grip found
 *                      False otherwise
 * 
 * Exceptions:
 * - CODESIGNER_ERR_LISTKEYS - gpg list keys could not be executed
 */
bool GpgCodeSigner::getKeyGrip(const char * userId, StringBuffer & keygrip)
{
    initGpg();
    keygrip.clear();

    StringBuffer cmd;
    if (isGpgV1)
        cmd.appendf("gpg %s --list-secret-keys \"=%s\"", gpgOptions.str(), userId); // = means exact match
    else
        cmd.appendf("gpg %s --list-secret-keys --with-keygrip \"=%s\"", gpgOptions.str(), userId); // = means exact match

    StringBuffer output, errmsg;
    int ret = runExternalCommand(output, errmsg, cmd.str(), nullptr);
    if (ret != 0)
    {
        if (strstr(errmsg.str(), "No secret key")==nullptr)
            throw makeStringExceptionV(MSGAUD_user, CODESIGNER_ERR_LISTKEYS, "List keys failed: %s (%d)", errmsg.str(), ret);
        return false;
    }

    if(strstr(output.str(), userId) == nullptr)
        return false;
    auto kgptr = strstr(output.str(), "Keygrip = ");
    if (kgptr)
        keygrip.append(40, kgptr+10);
    else
        return false;
    return true;
}

/**
 * Clear the passphrase cached with agent of specified user
 *
 * @param key           Keygrip of user
 *
 * Note: this may fail silently
 */
void GpgCodeSigner::clearPassphrase(const char * key)
{
    initGpg();
    StringBuffer output, errmsg;
    VStringBuffer cmd("gpg-connect-agent %s \"clear_passphrase --mode=normal %s\" /bye", gpgOptions.str(), key);
    runExternalCommand(output, errmsg, cmd.str(), nullptr);
}
