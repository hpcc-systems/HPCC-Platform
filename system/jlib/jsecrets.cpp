/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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

#include "platform.h"
#include "jlog.hpp"
#include "jutil.hpp"
#include "jexcept.hpp"
#include "jmutex.hpp"
#include "jfile.hpp"
#include "jptree.hpp"
#include "jerror.hpp"
#include "jsecrets.hpp"

#include "build-config.h"

//including cpp-httplib single header file REST client
//  doesn't work with format-nonliteral as an error
//
#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wformat-nonliteral"
#endif

#ifdef _USE_OPENSSL
#define CPPHTTPLIB_OPENSSL_SUPPORT
#endif

#undef INVALID_SOCKET
#include "httplib.h"

#if defined(__clang__) || defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

#include <vector>

enum class CVaultKind { kv_v1, kv_v2 };

CVaultKind getSecretType(const char *s)
{
    if (isEmptyString(s))
        return CVaultKind::kv_v2;
    if (streq(s, "kv_v1"))
        return CVaultKind::kv_v1;
    return CVaultKind::kv_v2;
}
interface IVaultManager : extends IInterface
{
    virtual bool getCachedSecretFromVault(const char *category, const char *vaultId, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) = 0;
    virtual bool requestSecretFromVault(const char *category, const char *vaultId, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) = 0;
    virtual bool getCachedSecretByCategory(const char *category, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) = 0;
    virtual bool requestSecretByCategory(const char *category, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) = 0;
};

static CriticalSection secretCacheCS;
static Owned<IPropertyTree> secretCache;
static Owned<IVaultManager> vaultManager;

MODULE_INIT(INIT_PRIORITY_SYSTEM)
{
    secretCache.setown(createPTree());
    return true;
}

MODULE_EXIT()
{
    vaultManager.clear();
    secretCache.clear();
}

static void splitUrlAddress(const char *address, size_t len, StringBuffer &host, StringBuffer *port)
{
    if (!address || len==0)
        return;
    const char *sep = (const char *)memchr(address, ':', len);
    if (!sep)
        host.append(len, address);
    else
    {
        host.append(sep - address, address);
        len = len - (sep - address) - 1;
        if (port)
            port->append(len, sep+1);
        else
            host.append(':').append(len, sep+1);
    }
}

static void splitUrlAuthority(const char *authority, size_t authorityLen, StringBuffer &user, StringBuffer &password, StringBuffer &host, StringBuffer *port)
{
    if (!authority || authorityLen==0)
        return;
    const char *at = (const char *) memchr(authority, '@', authorityLen);
    if (!at)
        splitUrlAddress(authority, authorityLen, host, port);
    else
    {
        size_t userinfoLen = (at - authority);
        splitUrlAddress(at+1, authorityLen - userinfoLen - 1, host, port);
        const char *sep = (const char *) memchr(authority, ':', at - authority);
        if (!sep)
            user.append(at-authority, authority);
        else
        {
            user.append(sep-authority, authority);
            size_t passwordLen = (at - sep - 1);
            password.append(passwordLen, sep+1);
        }
    }
}

static inline void extractUrlProtocol(const char *&url, StringBuffer *scheme)
{
    if (!url)
        throw makeStringException(-1, "Invalid empty URL");
    if (0 == strnicmp(url, "HTTPS://", 8))
    {
        url+=8;
        if (scheme)
            scheme->append("https://");
    }
    else if (0 == strnicmp(url, "HTTP://", 7))
    {
        url+=7;
        if (scheme)
            scheme->append("http://");
    }
    else
        throw MakeStringException(-1, "Invalid URL, protocol not recognized %s", url);
}

static void splitUrlSections(const char *url, const char * &authority, size_t &authorityLen, StringBuffer &fullpath, StringBuffer *scheme)
{
    extractUrlProtocol(url, scheme);
    const char* path = strchr(url, '/');
    authority = url;
    if (!path)
        authorityLen = strlen(authority);
    else
    {
        authorityLen = path-url;
        fullpath.append(path);
    }
}

extern jlib_decl void splitFullUrl(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &host, StringBuffer &port, StringBuffer &path)
{
    const char *authority = nullptr;
    size_t authorityLen = 0;
    splitUrlSections(url, authority, authorityLen, path, nullptr);
    splitUrlAuthority(authority, authorityLen, user, password, host, &port);
}

extern jlib_decl void splitUrlSchemeHostPort(const char *url, StringBuffer &user, StringBuffer &password, StringBuffer &schemeHostPort, StringBuffer &path)
{
    const char *authority = nullptr;
    size_t authorityLen = 0;
    splitUrlSections(url, authority, authorityLen, path, &schemeHostPort);
    splitUrlAuthority(authority, authorityLen, user, password, schemeHostPort, nullptr);
}

//---------------------------------------------------------------------------------------------------------------------


static StringBuffer secretDirectory;
static CriticalSection secretCS;

//there are various schemes for renewing kubernetes secrets and they are likely to vary greatly in how often
//  a secret gets updated this timeout determines the maximum amount of time before we'll pick up a change
//  10 minutes for now we can change this as we gather more experience and user feedback
static unsigned secretTimeoutMs = 10 * 60 * 1000;

extern jlib_decl unsigned getSecretTimeout()
{
    return secretTimeoutMs;
}

extern jlib_decl void setSecretTimeout(unsigned timeoutMs)
{
    secretTimeoutMs = timeoutMs;
}

extern jlib_decl void setSecretMount(const char * path)
{
    if (!path)
    {
        getPackageFolder(secretDirectory);
        addPathSepChar(secretDirectory).append("secrets");
    }
    else
        secretDirectory.set(path);
}

static inline bool checkSecretExpired(unsigned created)
{
    if (!created)
        return false;
    unsigned age = msTick() - created;
    return age > getSecretTimeout();
}

class CVault
{
private:
    bool useKubernetesAuth = true;
    CVaultKind kind;
    CriticalSection vaultCS;
    Owned<IPropertyTree> cache;

    StringBuffer schemeHostPort;
    StringBuffer path;
    StringBuffer username;
    StringBuffer password;
    StringAttr name;
    StringAttr role;
    StringAttr token;

public:
    CVault(IPropertyTree *vault)
    {
        cache.setown(createPTree());
        StringBuffer url;
        replaceEnvVariables(url, vault->queryProp("@url"), false);
        if (url.length())
            splitUrlSchemeHostPort(url.str(), username, password, schemeHostPort, path);
        name.set(vault->queryProp("@name"));
        kind = getSecretType(vault->queryProp("@kind"));
        if (vault->hasProp("@role"))
            role.set(vault->queryProp("@role"));
        else
            role.set("hpcc-vault-access");
        if (vault->hasProp("@client-secret"))
        {
            useKubernetesAuth = false;
            //for now only support direct access token.  we can support other combinations for example login token, ldap login, etc later.
            Owned<IPropertyTree> clientSecret = getLocalSecret("system", vault->queryProp("@client-secret"));
            if (clientSecret)
                token.set(clientSecret->queryProp("token"));
        }
    }
    CVaultKind getVaultKind() const { return kind; }
    void kubernetesLogin()
    {
        CriticalBlock block(vaultCS);
        if (token.length())
            return;
        StringBuffer login_token;
        login_token.loadFile("/var/run/secrets/kubernetes.io/serviceaccount/token");
        if (login_token.length())
        {
            std::string json;
            json.append("{\"jwt\": \"").append(login_token.str()).append("\", \"role\": \"").append(role.str()).append("\"}");
            httplib::Client cli(schemeHostPort.str());
            if (username.length() && password.length())
                cli.set_basic_auth(username, password);
            httplib::Result res = cli.Post("/v1/auth/kubernetes/login", json, "application/json");
            if (res)
            {
                if (res->status == 200)
                {
                    const char *response = res->body.c_str();
                    if (!isEmptyString(response))
                    {
                        Owned<IPropertyTree> respTree = createPTreeFromJSONString(response);
                        if (respTree)
                            token.set(respTree->queryProp("auth/client_token"));
                    }
                }
                else
                {
                    Owned<IException> e = MakeStringException(0, "Vault kube auth error [%d](%d) - vault: %s - response: %s", res->status, res.error(), name.str(), res->body.c_str());
                    OWARNLOG(e);
                    throw e.getClear();
                }
            }
        }
    }
    bool getCachedSecret(CVaultKind &rkind, StringBuffer &content, const char *secret, const char *version)
    {
        CriticalBlock block(vaultCS);
        IPropertyTree *tree = cache->queryPropTree(secret);
        if (tree)
        {
            VStringBuffer vername("v.%s", isEmptyString(version) ? "latest" : version);
            IPropertyTree *envelope = tree->queryPropTree(vername);
            if (!envelope)
                return false;
            if (checkSecretExpired((unsigned) envelope->getPropInt("@created")))
            {
                tree->removeTree(envelope);
                return false;
            }
            const char *s = envelope->queryProp("");
            if (!isEmptyString(s))
            {
                rkind = kind;
                content.append(s);
                return true;
            }
        }
        return false;
    }
    void addCachedSecret(const char *content, const char *secret, const char *version)
    {
        VStringBuffer vername("v.%s", isEmptyString(version) ? "latest" : version);
        Owned<IPropertyTree> envelope = createPTree(vername);
        envelope->setPropInt("@created", (int) msTick());
        envelope->setProp("", content);
        {
            CriticalBlock block(vaultCS);
            IPropertyTree *parent = ensurePTree(cache, secret);
            parent->setPropTree(vername, envelope.getClear());
        }
    }
    bool requestSecret(CVaultKind &rkind, StringBuffer &content, const char *secret, const char *version)
    {
        if (isEmptyString(secret))
            return false;
        if (useKubernetesAuth && token.isEmpty())
            kubernetesLogin();
        if (token.isEmpty())
        {
            Owned<IException> e = MakeStringException(0, "Vault auth error - vault: %s - vault access token not provided", name.str());
            OERRLOG(e);
            throw e.getClear();
        }
        StringBuffer location(path);
        location.replaceString("${secret}", secret);
        location.replaceString("${version}", version ? version : "1");

        httplib::Client cli(schemeHostPort.str());
        if (username.length() && password.length())
            cli.set_basic_auth(username.str(), password.str());

        httplib::Headers headers = {
            { "X-Vault-Token", token.str() }
        };

        if (httplib::Result res = cli.Get(location, headers))
        {
            if (res->status == 200)
            {
                rkind = kind;
                content.append(res->body.c_str());
                addCachedSecret(content.str(), secret, version);
                return true;
            }
            else
            {
                DBGLOG("Vault %s error accessing secret %s.%s [%d](%d) - response: %s", name.str(), secret, version ? version : "", res->status, res.error(), res->body.c_str());
            }
        }
        return false;
    }
};

class CVaultSet
{
private:
    std::map<std::string, std::unique_ptr<CVault>> vaults;
public:
    CVaultSet()
    {
    }
    void addVault(IPropertyTree *vault)
    {
        const char *name = vault->queryProp("@name");
        if (!isEmptyString(name))
            vaults.emplace(name, std::unique_ptr<CVault>(new CVault(vault)));
    }
    bool getCachedSecret(CVaultKind &kind, StringBuffer &content, const char *secret, const char *version)
    {
        auto it = vaults.begin();
        for (; it != vaults.end(); it++)
        {
            if (it->second->getCachedSecret(kind, content, secret, version))
                return true;
        }
        return false;
    }
    bool requestSecret(CVaultKind &kind, StringBuffer &content, const char *secret, const char *version)
    {
        auto it = vaults.begin();
        for (; it != vaults.end(); it++)
        {
            if (it->second->requestSecret(kind, content, secret, version))
                return true;
        }
        return false;
    }
    bool getCachedSecretFromVault(const char *vaultId, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version)
    {
        if (isEmptyString(vaultId))
            return false;
        auto it = vaults.find(vaultId);
        if (it == vaults.end())
            return false;
        return it->second->getCachedSecret(kind, content, secret, version);
    }
    bool requestSecretFromVault(const char *vaultId, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version)
    {
        if (isEmptyString(vaultId))
            return false;
        auto it = vaults.find(vaultId);
        if (it == vaults.end())
            return false;
        return it->second->requestSecret(kind, content, secret, version);
    }
};

class CVaultManager : public CInterfaceOf<IVaultManager>
{
private:
    std::map<std::string, std::unique_ptr<CVaultSet>> categories;
public:
    CVaultManager()
    {
        IPropertyTree *config = nullptr;
        try
        {
            config = queryComponentConfig().queryPropTree("vaults");
        }
        catch (IException * e)
        {
            EXCLOG(e);
            e->Release();
        }
        if (!config)
            return;
        Owned<IPropertyTreeIterator> iter = config->getElements("*");
        ForEach (*iter)
        {
            IPropertyTree &vault = iter->query();
            const char *category = vault.queryName();
            auto it = categories.find(category);
            if (it == categories.end())
            {
                auto placed = categories.emplace(category, std::unique_ptr<CVaultSet>(new CVaultSet()));
                if (placed.second)
                    it = placed.first;
            }
            if (it != categories.end())
                it->second->addVault(&vault);
        }
    }
    bool getCachedSecretFromVault(const char *category, const char *vaultId, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) override
    {
        if (isEmptyString(category))
            return false;
        auto it = categories.find(category);
        if (it == categories.end())
            return false;
        return it->second->getCachedSecretFromVault(vaultId, kind, content, secret, version);
    }
    bool requestSecretFromVault(const char *category, const char *vaultId, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) override
    {
        if (isEmptyString(category))
            return false;
        auto it = categories.find(category);
        if (it == categories.end())
            return false;
        return it->second->requestSecretFromVault(vaultId, kind, content, secret, version);
    }

    bool getCachedSecretByCategory(const char *category, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) override
    {
        if (isEmptyString(category))
            return false;
        auto it = categories.find(category);
        if (it == categories.end())
            return false;
        return it->second->getCachedSecret(kind, content, secret, version);
    }
    bool requestSecretByCategory(const char *category, CVaultKind &kind, StringBuffer &content, const char *secret, const char *version) override
    {
        if (isEmptyString(category))
            return false;
        auto it = categories.find(category);
        if (it == categories.end())
            return false;
        return it->second->requestSecret(kind, content, secret, version);
    }
};

IVaultManager *ensureVaultManager()
{
    CriticalBlock block(secretCS);
    if (!vaultManager)
        vaultManager.setown(new CVaultManager());
    return vaultManager;
}

static IPropertyTree *getCachedLocalSecret(const char *category, const char *name)
{
    if (isEmptyString(name))
        return nullptr;
    Owned<IPropertyTree> secret;
    {
        CriticalBlock block(secretCacheCS);
        IPropertyTree *tree = secretCache->queryPropTree(category);
        if (!tree)
            return nullptr;
        secret.setown(tree->getPropTree(name));
        if (secret)
        {
            if (checkSecretExpired((unsigned) secret->getPropInt("@created")))
            {
                secretCache->removeProp(name);
                return nullptr;
            }
            return secret.getClear();
        }
    }
    return nullptr;
}

static void addCachedLocalSecret(const char *category, const char *name, IPropertyTree *secret)
{
    if (!secret || isEmptyString(name) || isEmptyString(category))
        return;
    secret->setPropInt("@created", (int)msTick());
    {
        CriticalBlock block(secretCacheCS);
        IPropertyTree *tree = ensurePTree(secretCache, category);
        tree->setPropTree(name, LINK(secret));
    }
}

static const char *ensureSecretDirectory()
{
    CriticalBlock block(secretCS);
    if (secretDirectory.isEmpty())
        setSecretMount(nullptr);
    return secretDirectory;
}

static IPropertyTree *loadLocalSecret(const char *category, const char * name)
{
    StringBuffer path;
    addPathSepChar(path.append(ensureSecretDirectory())).append(category).append(PATHSEPCHAR).append(name).append(PATHSEPCHAR);
    Owned<IDirectoryIterator> entries = createDirectoryIterator(path);
    if (!entries || !entries->first())
        return nullptr;
    Owned<IPropertyTree> tree = createPTree(name);
    tree->setPropInt("@created", (int) msTick());
    ForEach(*entries)
    {
        if (entries->isDir())
            continue;
        StringBuffer name;
        entries->getName(name);
        if (!validateXMLTag(name))
            continue;
        MemoryBuffer content;
        Owned<IFileIO> io = entries->get().open(IFOread);
        read(io, 0, (size32_t)-1, content);
        if (!content.length())
            continue;
        tree->setPropBin(name, content.length(), content.bufferBase());
    }
    addCachedLocalSecret(category, name, tree);
    return tree.getClear();
}

extern jlib_decl IPropertyTree *getLocalSecret(const char *category, const char * name)
{
    Owned<IPropertyTree> tree = getCachedLocalSecret(category, name);
    if (tree)
        return tree.getClear();
    return loadLocalSecret(category, name);
}

static IPropertyTree *createPTreeFromVaultSecret(const char *content, CVaultKind kind)
{
    if (isEmptyString(content))
        return nullptr;

    Owned<IPropertyTree> tree = createPTreeFromJSONString(content);
    if (!tree)
        return nullptr;
    switch (kind)
    {
        case CVaultKind::kv_v1:
            tree.setown(tree->getPropTree("data"));
            break;
        default:
        case CVaultKind::kv_v2:
            tree.setown(tree->getPropTree("data/data"));
            break;
    }
    return tree.getClear();
}
static IPropertyTree *getCachedVaultSecret(const char *category, const char *vaultId, const char * name, const char *version)
{
    CVaultKind kind;
    StringBuffer json;
    IVaultManager *vaultmgr = ensureVaultManager();
    if (isEmptyString(vaultId))
    {
        if (!vaultmgr->getCachedSecretByCategory(category, kind, json, name, version))
            return nullptr;
    }
    else
    {
        if (!vaultmgr->getCachedSecretFromVault(category, vaultId, kind, json, name, version))
            return nullptr;
    }
    return createPTreeFromVaultSecret(json.str(), kind);
}

static IPropertyTree *requestVaultSecret(const char *category, const char *vaultId, const char * name, const char *version)
{
    CVaultKind kind;
    StringBuffer json;
    IVaultManager *vaultmgr = ensureVaultManager();
    if (isEmptyString(vaultId))
    {
        if (!vaultmgr->requestSecretByCategory(category, kind, json, name, version))
            return nullptr;
    }
    else
    {
        if (!vaultmgr->requestSecretFromVault(category, vaultId, kind, json, name, version))
            return nullptr;
    }
    return createPTreeFromVaultSecret(json.str(), kind);
}

extern jlib_decl IPropertyTree *getVaultSecret(const char *category, const char *vaultId, const char * name, const char *version)
{
    CVaultKind kind;
    StringBuffer json;
    IVaultManager *vaultmgr = ensureVaultManager();
    if (isEmptyString(vaultId))
    {
        if (!vaultmgr->getCachedSecretByCategory(category, kind, json, name, version))
            vaultmgr->requestSecretByCategory(category, kind, json, name, version);
    }
    else
    {
        if (!vaultmgr->getCachedSecretFromVault(category, vaultId, kind, json, name, version))
            vaultmgr->requestSecretFromVault(category, vaultId, kind, json, name, version);
    }
    return createPTreeFromVaultSecret(json.str(), kind);
}

extern jlib_decl IPropertyTree *getSecret(const char *category, const char * name)
{
    //check for any chached first
    Owned<IPropertyTree> secret = getCachedLocalSecret(category, name);
    if (!secret)
        secret.setown(getCachedVaultSecret(category, nullptr, name, nullptr));
    //now check local, then vaults
    if (!secret)
        secret.setown(loadLocalSecret(category, name));
    if (!secret)
        secret.setown(requestVaultSecret(category, nullptr, name, nullptr));
    return secret.getClear();
}

extern jlib_decl bool getSecretKeyValue(MemoryBuffer & result, IPropertyTree *secret, const char * key)
{
    IPropertyTree *tree = secret->queryPropTree(key);
    if (tree)
        return tree->getPropBin(nullptr, result);
    return false;
}

extern jlib_decl bool getSecretKeyValue(StringBuffer & result, IPropertyTree *secret, const char * key)
{
    IPropertyTree *tree = secret->queryPropTree(key);
    if (!tree)
        return false;
    if (tree->isBinary(nullptr))
    {
        MemoryBuffer mb;
        tree->getPropBin(nullptr, mb);
        //caller implies it's a string
        result.append(mb.length(), mb.toByteArray());
        return true;
    }
    const char *value = tree->queryProp(nullptr);
    if (value)
    {
        result.append(value);
        return true;
    }
    return false;
}

extern jlib_decl bool getSecretValue(StringBuffer & result, const char *category, const char * name, const char * key, bool required)
{
    Owned<IPropertyTree> secret = getSecret(category, name);
    if (required && !secret)
        throw MakeStringException(-1, "secret %s.%s not found", category, name);
    bool found = getSecretKeyValue(result, secret, key);
    if (required && !found)
        throw MakeStringException(-1, "secret %s.%s missing key %s", category, name, key);
    return true;
}

