/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
#include "jlib.hpp"
#include "jcontainerized.hpp"
#include "jstring.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jsort.hpp"
#include "jprop.hpp"
#include "jregexp.hpp"
#include "jset.hpp"

#include "mpbase.hpp"
#include "dautils.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "daclient.hpp"
#include "rmtclient.hpp"

#include <vector>

#ifdef _DEBUG
//#define TEST_DEADLOCK_RELEASE
#endif

#define EXTERNAL_SCOPE      "file"
#define PLANE_SCOPE         "plane"
#define FOREIGN_SCOPE       "foreign"
#define REMOTE_SCOPE        "remote"
#define SELF_SCOPE          "."
#define SDS_DFS_ROOT        "Files" // followed by scope/name
#define SDS_RELATIONSHIPS_ROOT  "Files/Relationships"
#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define MIN_REDIRECTION_LOAD_INTERVAL 1000

static IPropertyTree *getPlaneHostGroup(IPropertyTree *plane)
{
    if (plane->hasProp("@hostGroup"))
        return getHostGroup(plane->queryProp("@hostGroup"), true);
    else if (plane->hasProp("hosts"))
        return LINK(plane); // plane itself holds 'hosts'
    return nullptr;
}

bool isHostInPlane(IPropertyTree *plane, const char *host, bool ipMatch)
{
    Owned<IPropertyTree> planeGroup = getPlaneHostGroup(plane);
    if (!planeGroup)
        return false;
    Owned<IPropertyTreeIterator> hostsIter = planeGroup->getElements("hosts");
    SocketEndpoint hostEp;
    if (ipMatch)
        hostEp.set(host);
    ForEach (*hostsIter)
    {
        const char *planeHost = hostsIter->query().queryProp(nullptr);
        if (ipMatch)
        {
            SocketEndpoint planeHostEp(planeHost);
            if (planeHostEp.ipequals(hostEp))
                return true;
        }
        else if (streq(planeHost, host))
            return true;
    }
    return false;
}

bool getPlaneHost(StringBuffer &host, IPropertyTree *plane, unsigned which)
{
    Owned<IPropertyTree> hostGroup = getPlaneHostGroup(plane);
    if (!hostGroup)
        return false;

    unsigned maxHosts = hostGroup->getCount("hosts");
    if (which >= maxHosts)
        throw makeStringExceptionV(0, "getPlaneHost: index %u out of range 1..%u", which, maxHosts);
    VStringBuffer xpath("hosts[%u]", which+1); // which is 0 based
    host.append(hostGroup->queryProp(xpath));
    return true;
}

void getPlaneHosts(StringArray &hosts, IPropertyTree *plane)
{
    Owned<IPropertyTree> hostGroup = getPlaneHostGroup(plane);
    if (hostGroup)
    {
        Owned<IPropertyTreeIterator> hostsIter = hostGroup->getElements("hosts");
        ForEach (*hostsIter)
            hosts.append(hostsIter->query().queryProp(nullptr));
    }
}

IPropertyTreeIterator * getDropZonePlanesIterator(const char * name)
{
    return getPlanesIterator("lz", name);
}

IPropertyTree * getDropZonePlane(const char * name)
{
    if (isEmptyString(name))
        throw makeStringException(-1, "Drop zone name required");
    Owned<IPropertyTreeIterator> iter = getDropZonePlanesIterator(name);
    return iter->first() ? &iter->get() : nullptr;
}

bool isPathInPlane(IPropertyTree *plane, const char *path)
{
    if (isEmptyString(path))
        return true;

    const char *prefix = plane->queryProp("@prefix");
    if (isEmptyString(prefix))
        return false; //prefix is empty, path is not - can't match.

    while (*prefix && *prefix == *path)
    {
        path++;
        prefix++;
    }
    if (0 == *prefix)
    {
        if (0 == *path || isPathSepChar(*path))
            return true;
        if (isPathSepChar(*(path - 1))) //implies both last characters of prefix and path were '/'
            return true;
    }
    else if (0 == *path && isPathSepChar(*prefix) && (0 == *(prefix + 1)))
        return true;
    return false;
}

bool validateDropZone(IPropertyTree * plane, const char * path, const char * host, bool ipMatch)
{
    if (!isEmptyString(host))
    {
        if (!isHostInPlane(plane, host, ipMatch))
            return false;
    }
    else if (plane->hasProp("@hostGroup") || plane->hasProp("hosts"))
        return false;

    //Match path
    return isPathInPlane(plane, path);
}

IPropertyTree * findPlane(const char *category, const char * path, const char * host, bool ipMatch, bool mustMatch)
{
    if (strsame(host, "localhost"))
        host = nullptr;
    StringBuffer xpath("storage/planes");
    if (!isEmptyString(category))
        xpath.appendf("[@category='%s']", category);
    Owned<IPropertyTreeIterator> iter = getGlobalConfigSP()->getElements(xpath);
    ForEach(*iter)
    {
        IPropertyTree & plane = iter->query();
        if (validateDropZone(&plane, path, host, ipMatch))
            return LINK(&plane);
    }
    if (mustMatch)
        throw makeStringExceptionV(-1, "DropZone not found for host '%s' path '%s'.",
            isEmptyString(host) ? "unspecified" : host, isEmptyString(path) ? "unspecified" : path);
    return nullptr;
}

IPropertyTree * findDropZonePlane(const char * path, const char * host, bool ipMatch, bool mustMatch)
{
    return findPlane("lz", path, host, ipMatch, mustMatch);
}

bool allowForeign()
{
    StringBuffer optValue;
    // NB: component setting takes precedence over global
    getComponentConfigSP()->getProp("expert/@allowForeign", optValue);
    if (!optValue.isEmpty())
        return strToBool(optValue);

    getGlobalConfigSP()->getProp("expert/@allowForeign", optValue);
    if (!optValue.isEmpty())
        return strToBool(optValue);
    // default denied in cloud, allowed in bare-metal
    return isContainerized() ? false : true;
}

extern da_decl const char *queryDfsXmlBranchName(DfsXmlBranchKind kind)
{
    switch (kind) {
    case DXB_File:          return "File";
    case DXB_SuperFile:     return "SuperFile";
    case DXB_Collection:    return "Collection";
    case DXB_Scope:         return "Scope";
    case DXB_Internal:      return "HpccInternal";
    }
    assertex(!"unknown DFS XML branch name");
    return "UNKNOWN";
}

extern da_decl DfsXmlBranchKind queryDfsXmlBranchType(const char *typeStr)
{
    if (isEmptyString(typeStr))
        throw makeStringException(0, "Blank DFS xml branch type");
    if (strieq(typeStr, "File"))
        return DXB_File;
    else if (strieq(typeStr, "SuperFile"))
        return DXB_SuperFile;
    else if (strieq(typeStr, "Collection"))
        return DXB_Collection;
    else if (strieq(typeStr, "Scope"))
        return DXB_Scope;
    else if (strieq(typeStr, "HpccInternal"))
        return DXB_Internal;
    else
        throw makeStringExceptionV(0, "Unknown DFS xml Branch type: %s", typeStr);
}


static const char *toLower(const char *s,StringBuffer &str)
{
    return str.clear().append(s).toLowerCase().str();
}

static const char *toLowerTrim(const char *s,StringBuffer &str)
{
    return str.clear().append(s).trim().toLowerCase().str();
}

inline void skipSp(const char *&s)
{
    while (isspace(*s))
        s++;
}

static constexpr const char *invalids = "*\"/:<>?\\|";
bool validFNameChar(char c)
{
    return (c>=32 && c<127 && !strchr(invalids, c));
}

/**
 * Multi-distributed logical file names. Used by SuperFiles to
 * account for the names expanded from wildcards or lists, ex.
 * DATASET('scope::{a, b, c}', ...)
 *
 * This helper class is used for temporary inline super-files
 * only.
 */

class CMultiDLFN
{
    std::vector<CDfsLogicalFileName> dlfns;
    bool expanded;
public:
    CMultiDLFN(const char *_prefix,const StringArray &lfns)
    {
        unsigned c = lfns.ordinality();
        StringBuffer lfn(_prefix);
        size32_t len = lfn.length();
        expanded = true;
        for (unsigned i=0;i<c;i++) {    //Populate CDfsLogicalFileName array with each logical filespec
            const char * s = lfns.item(i);
            skipSp(s);
            if (*s=='~')
                s++;//scope provided, create CDfsLogicalFileName as-is
            else {
                lfn.setLength(len);//scope not specified, append it before creating CDfsLogicalFileName
                lfn.append(s);
                s = lfn.str();
            }
            CDfsLogicalFileName lfn;
            lfn.setAllowWild(true);
            lfn.set(s);
            dlfns.push_back(lfn);
            if (expanded && (strchr(s,'*') || strchr(s,'?')))
                expanded = false;
        }
    }

    CMultiDLFN(CMultiDLFN &other)
    {
        expanded = other.expanded;
        ForEachItemIn(i,other)
            dlfns.push_back(other.item(i));
    }

    void expand(IUserDescriptor *_udesc)
    {
        if (expanded)
            return;
        StringArray lfnExpanded;
        StringBuffer tmp;
        for (unsigned idx=0; idx < dlfns.size(); idx++)
        {
            const char *suffix = dlfns.at(idx).get();
            if (strchr(suffix,'*') || strchr(suffix,'?'))
            {
                tmp.clear();
                if (*suffix=='~')
                    tmp.append((strcmp(suffix+1,"*")==0) ? "?*" : suffix+1);
                else
                    tmp.append(suffix);
                tmp.clip().toLowerCase();
                CDfsLogicalFileName sub;
                sub.setAllowWild(true);
                sub.set(tmp);
                Owned<IDFAttributesIterator> iter;
                SocketEndpoint foreignEp;
                if (sub.isForeign(&foreignEp))
                {
                    sub.get(tmp.clear(), true);
                    Owned<INode> foreignNode = createINode(foreignEp);
                    iter.setown(queryDistributedFileDirectory().getDFAttributesIterator(tmp.str(),_udesc,false,true,foreignNode));
                }
                else
                    iter.setown(queryDistributedFileDirectory().getDFAttributesIterator(tmp.str(),_udesc,false,true,nullptr));
                ForEach(*iter)
                {
                    IPropertyTree &attr = iter->query();
                    const char *name = attr.queryProp("@name");
                    if (!name||!*name)
                        continue;
                    tmp.clear().append('~'); // need leading ~ otherwise will get two prefixes
                    if (sub.isForeign())
                    {
                        tmp.append(FOREIGN_SCOPE "::");
                        foreignEp.getEndpointHostText(tmp).append("::");
                    }
                    else if (sub.isRemote())
                    {
                        tmp.append(REMOTE_SCOPE "::");
                        foreignEp.getEndpointHostText(tmp).append("::");
                    }
                    tmp.append(name);
                    lfnExpanded.append(tmp.str());
                }
            }
            else
                lfnExpanded.append(suffix);
        }

        dlfns.clear();
        ForEachItemIn(i3,lfnExpanded)
        {
            CDfsLogicalFileName item;
            item.set(lfnExpanded.item(i3));
            dlfns.push_back(item);
        }
        expanded = true;
    }

    static CMultiDLFN *create(const char *_mlfn)
    {
        StringBuffer mlfn(_mlfn);
        mlfn.trim();
        if (mlfn.length()<=2)
            return NULL;
        const char *s = mlfn.str();
        if (s[mlfn.length()-1]!='}')
            return NULL;
        mlfn.remove(mlfn.length()-1,1);
        s = mlfn.str(); // prob not needed, but...
        const char *start = strchr(s,'{');
        if (!start)
            return NULL;
        StringArray lfns;
        lfns.appendList(start+1, ",");
        bool anywilds = false;
        ForEachItemIn(i1,lfns) {
            const char *suffix = lfns.item(i1);
            if (strchr(suffix,'*')||strchr(suffix,'?')) {
                anywilds = true;
                break;
            }
        }
        mlfn.setLength(start-s); // Just keep the prefix (anything before leading {)
        CMultiDLFN *ret =  new CMultiDLFN(mlfn.str(), lfns);
        if (ret->ordinality() || anywilds)
            return ret;
        delete ret;
        return NULL;
    }

    const CDfsLogicalFileName &item(unsigned idx)
    {
        assertex(idx < dlfns.size());
        return dlfns.at(idx);
    }

    inline unsigned ordinality() const { return dlfns.size(); }
    inline bool isExpanded()     const { return expanded; }
};


CDfsLogicalFileName::CDfsLogicalFileName()
{
    allowospath = false;
    allowWild = false;
    allowTrailingEmptyScope = false;
    multi = NULL;
    clear();
}

CDfsLogicalFileName::~CDfsLogicalFileName()
{
    delete multi;
}


void CDfsLogicalFileName::clear()
{
    lfn.clear();
    external = false;
    localpos = 0;
    tailpos = 0;
    delete multi;
    multi = NULL;
}

bool CDfsLogicalFileName::isSet() const
{
    const char *s = lfn.get();
    return s&&*s;
}

CDfsLogicalFileName & CDfsLogicalFileName::operator = (CDfsLogicalFileName const &from)
{
    set(from);
    return *this;
}

bool CDfsLogicalFileName::isExternalPlane() const
{
    return external && startsWithIgnoreCase(lfn, PLANE_SCOPE "::");
}

bool CDfsLogicalFileName::isRemote() const
{
    return external && startsWithIgnoreCase(lfn, REMOTE_SCOPE "::");
}

bool CDfsLogicalFileName::getExternalPlane(StringBuffer & plane) const
{
    if (!isExternalPlane())
        return false;

    const char * start = lfn.str() + strlen(PLANE_SCOPE "::");
    const char * end = strstr(start,"::");
    assertex(end);
    plane.append(end-start, start);
    return true;
}


bool CDfsLogicalFileName::getRemoteSpec(StringBuffer &remoteSvc, StringBuffer &logicalName) const
{
    if (!isRemote())
        return false;

    const char * start = lfn.str() + strlen(REMOTE_SCOPE "::");
    const char * end = strstr(start,"::");
    assertex(end);
    remoteSvc.append(end-start, start);
    logicalName.append(end+2);
    return true;
}


bool CDfsLogicalFileName::isExternalFile() const
{
    return external && startsWithIgnoreCase(lfn, EXTERNAL_SCOPE "::");
}

bool CDfsLogicalFileName::getExternalHost(StringBuffer & host) const
{
    if (!isExternalFile())
        return false;

    const char * start = lfn.str() + strlen(EXTERNAL_SCOPE "::");
    const char * end = strstr(start,"::");
    assertex(end);
    host.append(end-start, start);
    return true;
}


void CDfsLogicalFileName::set(const CDfsLogicalFileName &other)
{
    lfn.set(other.lfn);
    tailpos = other.tailpos;
    localpos = other.localpos;
    foreignep = other.foreignep;
    cluster.set(other.cluster);
    external = other.external;
    delete multi;
    if (other.multi)
        multi = new CMultiDLFN(*other.multi);
    else
        multi = NULL;
}

bool CDfsLogicalFileName::isForeign(SocketEndpoint *ep) const
{
    if (localpos!=0) // NB: localpos is position of logical filename following a foreign::<nodeip>:: qualifier.
    {
        if (ep)
            *ep = foreignep;
        return true;
    }
    if (multi)
    {
        if (!multi->isExpanded())
            throw MakeStringException(-1, "Must call CDfsLogicalFileName::expand() before calling CDfsLogicalFileName::isForeign(), wildcards are specified");
        ForEachItemIn(i1,*multi)
            if (multi->item(i1).isForeign(ep))        // if any are say all are
                return true;
    }
    return false;
}

bool CDfsLogicalFileName::isExpanded() const
{
    if (multi)
        return multi->isExpanded();
    return true;
}

void CDfsLogicalFileName::expand(IUserDescriptor *user)
{
    if (multi && !multi->isExpanded())
    {
        try
        {
            multi->expand(user);//expand wildcard specifications
            StringBuffer full("{");
            ForEachItemIn(i1,*multi)
            {
                if (i1)
                    full.append(',');
                const CDfsLogicalFileName &item = multi->item(i1);
                StringAttr norm;
                normalizeName(item.get(), norm, false, true);
                full.append(norm);
                if (item.isExternal())
                    external = external || item.isExternal();
            }
            full.append('}');
            lfn.set(full);
        }
        catch (IException *e)
        {
            StringBuffer err;
            e->errorMessage(err);
            IERRLOG("CDfsLogicalFileName::expand %s",err.str());
            throw;
        }
    }
}

inline void normalizeScope(const char *name, const char *scope, unsigned len, StringBuffer &res, bool strict, bool allowEmptyScope)
{
    while (len && isspace(scope[len-1]))
    {
        if (strict)
            throw MakeStringException(-1, "Scope contains trailing spaces in file name '%s'", name);
        len--;
    }
    while (len && isspace(scope[0]))
    {
        if (strict)
            throw MakeStringException(-1, "Scope contains leading spaces in file name '%s'", name);
        len--;
        scope++;
    }
    if (!len && !allowEmptyScope)
        throw MakeStringException(-1, "Scope is blank in file name '%s'", name);

    res.append(len, scope);
}

void normalizeNodeName(const char *node, unsigned len, SocketEndpoint &ep, bool strict)
{
    if (!strict)
    {
        while (isspace(*node))
        {
            node++;
            len--;
        }
    }

    StringBuffer nodename;
    nodename.append(len, node);
    if (!strict)
        nodename.clip();
    ep.set(nodename.str());
}


//s points to the second "::" in the external filename (file::ip or plane::<plane>::)
bool expandExternalPath(StringBuffer &dir, StringBuffer &tail, const char * filename, const char * s, bool iswin, IException **e)
{
    if (e)
        *e = NULL;
    if (!s) {
        if (e)
            *e = MakeStringException(-1,"Invalid format for external file (%s)",filename);
        return false;
    }
    if (s[2]=='>') {
        dir.append('/');
        tail.append(s+2);
        return true;
    }

    // check for ::c$/
    if (iswin&&(s[3]=='$'))
        s += 2;                 // no leading '\'
    const char *s1=s;
    const char *t1=NULL;
    for (;;) {
        s1 = strstr(s1,"::");
        if (!s1)
            break;
        t1 = s1;
        s1 = s1+2;
    }
    //The following code is never actually executed, since s always points at the leading '::'
    if (!t1||!*t1) {
        if (e)
            *e = MakeStringException(-1,"No directory specified in external file name (%s)",filename);
        return false;
    }
    size32_t odl = dir.length();
    bool start=true;
    while (s!=t1) {
        char c=*(s++);
        if (isPathSepChar(c)) {
            if (e)
                *e = MakeStringException(-1,"Path cannot contain separators, use '::' to separate directories: (%s)",filename);
            return false;
        }
        if ((c==':')&&(s!=t1)&&(*s==':')) {
            dir.append(iswin?'\\':'/');
            s++;
            //Disallow ::..:: to gain access to parent subdirectories
            if (strncmp(s, "..::", 4) == 0)
            {
                if (e)
                    *e = MakeStringException(-1,"External filename cannot contain relative path '..' (%s)", filename);
                return false;
            }
        }
        else if (c==':') {
            if (e)
                *e = MakeStringException(-1,"Path cannot contain single ':', use 'c$' to indicate 'c:' (%s)",filename);
            return false;
        }
        else if (iswin&&start&&(s!=t1)&&(*s=='$')) {
            dir.append(c).append(':');
            s++;
        }
        else {
            if ((c=='^')&&(s!=t1)) {
                c = toupper(*s);
                s++;
            }
            dir.append(c);
        }
        start = false;
    }
    t1+=2; // skip ::
    //Always ensure there is a // - if not directory is provided it will be in the root
    if ((dir.length()==0)||(!isPathSepChar(dir.charAt(dir.length()-1))))
        dir.append(iswin?'\\':'/');
    while (*t1) {
        char c = *(t1++);
        if ((c=='^')&&*t1) {
            c = toupper(*t1);
            t1++;
        }
        tail.append(c);
    }
    return true;
}

void CDfsLogicalFileName::normalizeName(const char *name, StringAttr &res, bool strict, bool nameIsRoot)
{
    // NB: If !strict(default) allows spaces to exist either side of scopes (no idea why would want to permit that, but preserving for bwrd compat.)
    StringBuffer nametmp;
    const char *ct = nullptr;
    bool wilddetected = false;
    if ('~' == *name) // allowed 1 leading ~
    {
        name++;
        if (!strict)
            skipSp(name);
    }
    const char *s = name;
    char c = *s;
    while (c)
    {
        switch (c)
        {
            case '@': ct = s; break;
            case ':': ct = nullptr; break;
            case '?':
            case '*': wilddetected = true; break;
            case '~':
                throw MakeStringException(-1, "Unexpected character '%c' in logical name '%s' detected", *s, name);
            case '>':
            {
                c = '\0'; // will cause break out of loop
                continue; // can't validate query syntax
            }
            default:
            {
                if (!validFNameChar(c))
                    throw MakeStringException(-1, "Unexpected character '%c' in logical name '%s' detected", *s, name);
            }
        }
        c = *++s;
    }
    if (!allowWild && wilddetected)
        throw MakeStringException(-1, "Wildcards not allowed in filename (%s)", name);
    if (ct&&(ct-name>=1)) // trailing @
    {
        if ((ct[1]=='@')||(ct[1]=='^')) // escape
        {
            nametmp.append(ct-name,name).append(ct+1);
            name = nametmp.str();
        }
        else
        {
            nametmp.append(ct+1);
            nametmp.trim().toLowerCase();
            if (nametmp.length())
                cluster.set(nametmp);
            nametmp.clear().append(ct-name,name);   // treat trailing @ as no cluster
            name = nametmp.str();
        }
    }
    if (!*name)
    {
        name = ".::_blank_";
        tailpos = 3;
        res.set(name);
    }
    else
    {
        StringBuffer str;
        s=strstr(name,"::");
        if (s)
        {
            bool skipScope = false;
            unsigned scopeStart = 0;
            if (s==name) // JCS: meaning name leads with "::", would have thought should be treated as invalid
                str.append('.');
            else
            {
                normalizeScope(name, name, s-name, str, strict, false);
                if (strieq(FOREIGN_SCOPE, str)) // normalize node
                {
                    const char *s1 = s+2;
                    const char *ns1 = strstr(s1,"::");
                    if (ns1)
                    {
                        normalizeNodeName(s1, ns1-s1, foreignep, strict);
                        if (!foreignep.isNull())
                        {
                            foreignep.getEndpointHostText(str.append("::"));
                            s = ns1;
                            localpos = str.length()+2;
                        }
                    }
                }
                else if (streq(SELF_SCOPE, str) && selfScopeTranslation)
                    skipScope = true;
            }
            for (;;)
            {
                s+=2;
                const char *ns = strstr(s,"::");
                if ((ns || str.length()>1) && skipScope && selfScopeTranslation)
                {
                    str.setLength(scopeStart);
                    skipScope = false;
                }
                else
                    str.append("::");
                if (!ns)
                    break;
                scopeStart = str.length();
                normalizeScope(name, s, ns-s, str, strict, false);
                unsigned scopeLen = str.length()-scopeStart;
                if ((1 == scopeLen) && (*SELF_SCOPE == str.charAt(str.length()-1)) && selfScopeTranslation)
                    skipScope = true;
                s = ns;
            }
        }
        else
        {
            s = name;
            if (nameIsRoot)
                str.append(".::");
        }
        tailpos = str.length();
        normalizeScope(name, s, strlen(name)-(s-name), str, strict, allowTrailingEmptyScope);
        unsigned scopeLen = str.length()-tailpos;
        if ((1 == scopeLen) && (*SELF_SCOPE == str.charAt(str.length()-1)))
            throw MakeStringException(-1, "Logical filename cannot end with scope \".\"");
        str.toLowerCase();
        res.set(str);
    }
}

bool CDfsLogicalFileName::normalizeExternal(const char * name, StringAttr &res, bool strict)
{
    // TODO Should check the name is a valid OS filename
    if ('~' == *name) // allowed 1 leading ~
    {
        name++;
        if (!strict)
            skipSp(name);
    }

    StringBuffer str;
    lfn.clear();
    const char *s=strstr(name,"::");
    enum { lfntype_file, lfntype_plane, lfntype_remote } lfnType;
    if (startsWithIgnoreCase(name, EXTERNAL_SCOPE "::"))
        lfnType = lfntype_file;
    else if (startsWithIgnoreCase(name, PLANE_SCOPE "::"))
        lfnType = lfntype_plane;
    else if (startsWithIgnoreCase(name, REMOTE_SCOPE "::"))
        lfnType = lfntype_remote;
    else
        return false;

    normalizeScope(name, name, s-name, str, strict, false); // "file" or "plane" or "remote"
    const char *s1 = s+2; // this will be the file host/ip, or plane name, or remote service name
    const char *ns1 = strstr(s1,"::");
    if (!ns1)
        return false;

    switch (lfnType)
    {
        case lfntype_file:
        {
            //syntax file::<ip>::<path>

            SocketEndpoint ep;
            normalizeNodeName(s1, ns1-s1, ep, strict);
            if (ep.isNull())
                return false;

            ep.getEndpointHostText(str.append("::"));
            if (ns1[2] == '>')
            {
                str.append("::");
                tailpos = str.length();
                str.append(ns1+2);
                res.set(str);
                return true;
            }

            // JCSMORE - really anything relying on using a CDfsLogicalFileName with wildcards should
            // be calling setAllowWild(true), but maintaining current semantics which has always allowed
            // wildcards because scopes were not being validated beyond this point before (HPCC-28885)
            allowWild = true;
            break;
        }
        case lfntype_plane:
        {
            //Syntax plane::<plane>::<path>

            StringBuffer planeName;
            normalizeScope(s1, s1, ns1-s1, planeName, strict, false);

            str.append("::").append(planeName);
            //Allow wildcards in plane path
            allowWild = true;
            break;
        }
        case lfntype_remote:
        {
            //Syntax plane::<remote>::<path>

            StringBuffer remoteSvc;
            normalizeScope(s1, s1, ns1-s1, remoteSvc, strict, false);

            str.append("::").append(remoteSvc);
            break;
        }
    }

    str.toLowerCase();
    str.append("::");
    // handle trailing scopes+name
    StringAttr tail;
    normalizeName(ns1+2, tail, strict, false); // +2 skipping "::", validated at start
    // normalizeName sets tailpos relative to ns1+2
    tailpos += str.length(); // length of <file|plane|remote>::<name>::
    str.append(tail);
    res.set(str);

    return true;
}

void CDfsLogicalFileName::set(const char *name, bool removeForeign, bool skipAddRootScopeIfNone)
{
    clear();
    if (!name)
        return;
    skipSp(name);
    if (allowospath&&(isAbsolutePath(name)||(stdIoHandle(name)>=0)||(strstr(name,"::")==nullptr)))
    {
        RemoteFilename rfn;
        rfn.setRemotePath(name);
        setExternal(rfn);
        return;
    }
    try
    {
        multi = CMultiDLFN::create(name);
    }
    catch (IException *e)
    {
        StringBuffer err;
        e->errorMessage(err);
        IERRLOG("CDfsLogicalFileName::set %s",err.str());
        e->Release();
    }
    if (multi)
    {
        StringBuffer full;
        full.append('{');
        ForEachItemIn(i1,*multi)
        {
            if (i1)
                full.append(',');
            const CDfsLogicalFileName &item = multi->item(i1);
            full.append(item.get());
            if (item.isExternal())
                external = external || item.isExternal();
        }
        full.append('}');
        lfn.set(full);
        return;
    }

    if (normalizeExternal(name, lfn, false))
        external = true;
    else
    {
        normalizeName(name, lfn, false, !skipAddRootScopeIfNone);
        if (removeForeign)
        {
            StringAttr _lfn = get(true);
            lfn.clear();
            lfn.set(_lfn);
        }
    }
}

bool CDfsLogicalFileName::setValidate(const char *lfn, bool removeForeign)
{
    try
    {
        set(lfn, removeForeign);
        return true;
    }
    catch (IException *e)
    {
        e->Release();
        return false;
    }
}


void CDfsLogicalFileName::set(const char *scopes,const char *tail)
{
    // probably not good with multi
    if (!scopes||!tail||(*tail=='~')) {
        set(tail);
        return;
    }
    skipSp(scopes);
    skipSp(tail);
    if (!*scopes||!*tail) {
        set(tail);
        return;
    }
    StringBuffer s(scopes);
    if ((s.length()<2)||(s.charAt(s.length()-1)!=':')||(s.charAt(s.length()-2)!=':'))
        s.append("::");
    s.append(tail);
    set(s.str());
    if (multi)
        DBGLOG("set with scopes called on multi-lfn %s",get());
}


void CDfsLogicalFileName::setForeign(const SocketEndpoint &daliep,bool checklocal)
{
    if (daliep.isNull())  {
        clearForeign();
        return;
    }
    if (isExternal()||(checklocal&&isForeign()))
        return;
    StringBuffer str(FOREIGN_SCOPE "::");
    daliep.getEndpointHostText(str);
    str.append("::");
    str.append(get(true));
    set(str);
}

static bool begins(const char *&ln,const char *pat)
{
    size32_t sz = strlen(pat);
    if (memicmp(ln,pat,sz)==0) {
        ln += sz;
        return true;
    }
    return false;
}

bool CDfsLogicalFileName::setFromXPath(const char *xpath)
{
    StringBuffer lName;
    const char *s = xpath;
    if (!begins(s, "/Files"))
        return false;

    while (*s && (begins(s, "/Scope[@name=\"") || begins(s, "/File[@name=\"") || begins(s, "/SuperFile[@name=\"")))
    {
        if (lName.length())
            lName.append("::");
        while (*s && (*s != '"'))
            lName.append(*(s++));
        if (*s == '"')
            s++;
        if (*s == ']')
            s++;
    }
    if (0 == lName.length())
        return false;
    return setValidate(lName);
}


void CDfsLogicalFileName::clearForeign()
{
    StringBuffer str;
    if (isForeign()) {
        str.append(get(true));
        set(str);
    }
}


unsigned CDfsLogicalFileName::multiOrdinality() const
{
    if (multi)
        return multi->ordinality();
    return 0;
}

bool CDfsLogicalFileName::isQuery() const
{
    const char *s = lfn.get();
    if (!s||!*s)
        return false;
    skipSp(s);
    if (*s=='~') {  // allowed 1 leading ~
        s++;
        skipSp(s);
    }
    const char *es = skipScope(s,EXTERNAL_SCOPE);
    if (es) {
        while (*es&&((*es!=':')||(es[1]!=':'))) // remove IP
            es++;
        if (*es&&(es[2]=='>'))
            return true;
    }
    return false;
}

const CDfsLogicalFileName &CDfsLogicalFileName::multiItem(unsigned idx) const
{
    assertex(multi);
    return multi->item(idx);
}

IPropertyTree *CDfsLogicalFileName::createSuperTree() const
{
    if (!multi)
        return NULL;
    IPropertyTree *ret = createPTree("SuperFile");
    unsigned numsub = 0;
    ForEachItemIn(i1,*multi) {
        const CDfsLogicalFileName &sub = multi->item(i1);
        IPropertyTree *st = createPTree();
        st->setProp("@name",sub.get());
        st->setPropInt("@num",++numsub);
        ret->addPropTree("SubFile",st);
    }
    ret->setProp("OrigName",get());
    ret->addPropInt("@numsubfiles",numsub);
    ret->setPropInt("@interleaved",2);
    ret->setProp("@name","__TEMP__");
    CDateTime dt;
    dt.setNow();
    StringBuffer str;
    ret->setProp("@modified",dt.getString(str).str());
    return ret;
}

static void convertPosixPathToLfn(StringBuffer &str,const char *path)
{
    if ((path[1]==':')&&(path[2]=='\\')) {
        str.append("::").append(path[0]).append('$');
        path+=2;
    }
    else if (!isPathSepChar(*path))
        str.append("::");
    StringBuffer urlencoded;
    if (*path == '$') {
        size32_t l = strlen(path);
        if (l>1) {
            str.append("$::");
            JBASE32_Encode(path+1, str);
            path = path+l;
        }
    }
    else {
        StringBuffer tmp;
        StringBuffer tmp2;
        decodeXML(path, tmp );          // allow user to use escape encoding
        if (isSpecialPath(path))
            str.append(path);
        else {
            encodeXML(tmp.str(), urlencoded, ENCODE_NEWLINES, tmp.length());
            path = urlencoded.str();
        }
    }
    if (!isSpecialPath(path)) {
        while (*path) {
            if (isPathSepChar(*path))
            {
                char next = *(path+1);
                if (next != '\0' && !isPathSepChar(next))
                    str.append("::");
            }
            else {
                if ((*path=='^')||isupper(*path))
                    str.append('^');
                str.append((char)tolower(*path));
            }
            path++;
        }
    }
}

void CDfsLogicalFileName::setPlaneExternal(const char *plane,const char *path)
{
    if (!isEmptyString(path)&&isPathSepChar(path[0])&&(path[0]==path[1]))
        throw makeStringExceptionV(-1,"Invalid path %s.",path);
    StringBuffer str(PLANE_SCOPE "::");
    str.append(plane);
    if (!isEmptyString(path))
        convertPosixPathToLfn(str,path);
    set(str.str());
}

void CDfsLogicalFileName::setExternal(const char *location,const char *path)
{
    if (isEmptyString(path))
        return;
    if (isPathSepChar(path[0])&&(path[0]==path[1])) {
        RemoteFilename rfn;
        rfn.setRemotePath(path);
        setExternal(rfn);  // overrides ip
        return;
    }
    StringBuffer str(EXTERNAL_SCOPE "::");
    str.append(location);
    convertPosixPathToLfn(str,path);
    set(str.str());
}

void CDfsLogicalFileName::setExternal(const SocketEndpoint &dafsip,const char *path)
{
    StringBuffer str;
    dafsip.getEndpointHostText(str);
    setExternal(str.str(),path);
}

void CDfsLogicalFileName::setExternal(const RemoteFilename &rfn)
{
    StringBuffer localpath;
    rfn.getLocalPath(localpath);
    setExternal(rfn.queryEndpoint(),localpath.str());
}

void CDfsLogicalFileName::setQuery(const char *location,const char *query)
{
    clear();
    if (!location||!query)
        return;
    skipSp(location);
    if (!*location)
        return;
    skipSp(query);
    StringBuffer str(EXTERNAL_SCOPE "::");
    str.append(location).clip().append("::>");
    tailpos = str.length()-1; // don't include '>'
    str.append(query).clip();
    lfn.set(str.str());
    external = true;
}

void CDfsLogicalFileName::setQuery(const SocketEndpoint &rfsep,const char *query)
{
    StringBuffer str;
    rfsep.getEndpointHostText(str);
    setQuery(str.str(),query);
}


void CDfsLogicalFileName::setCluster(const char *cname)
{
    if (multi)
        DBGLOG("setCluster called on multi-lfn %s",get());
    skipSp(cname);
    StringBuffer name(cname);
    cluster.set(name.clip().toLowerCase().str());
}



const char *CDfsLogicalFileName::get(bool removeforeign) const
{
    const char *ret = lfn.get();
    if (!ret)
        return "";
    if (removeforeign) {
        if (multi)
            DBGLOG("CDfsLogicalFileName::get with removeforeign set called on multi-lfn %s",get());
        ret += localpos;
    }
    return ret;
}

StringBuffer &CDfsLogicalFileName::get(StringBuffer &str, bool removeforeign, bool withCluster) const
{
    str.append(get(removeforeign));
    if (withCluster && cluster.length())
        str.append("@").append(cluster);
    return str;
}


const char *CDfsLogicalFileName::queryTail() const
{
    if (multi)
        DBGLOG("CDfsLogicalFileName::queryTail called on multi-lfn %s",get());
    const char *tail = get()+tailpos;
    if (strstr(tail,"::")!=NULL) {
        OERRLOG("Tail contains '::'!");
    }
    return get()+tailpos;
}

StringBuffer &CDfsLogicalFileName::getTail(StringBuffer &buf) const
{
    return buf.append(queryTail());
}

StringBuffer &CDfsLogicalFileName::getScopes(StringBuffer &buf,bool removeforeign) const
{
    if (multi)
        DBGLOG("CDfsLogicalFileName::getScopes called on multi-lfn %s",get());
    const char *s = lfn.get();
    if (!s||(tailpos<=2))
        return buf;
    size32_t sz = tailpos-2;
    if (removeforeign) {
        s += localpos;
        if (sz<=localpos)
            return buf;
        sz -= localpos;
    }
    return buf.append(sz,s);
}

unsigned CDfsLogicalFileName::numScopes(bool removeforeign) const
{
    const char *s =lfn.get();
    if (!s)
        return 0;
    if (multi)
        DBGLOG("CDfsLogicalFileName::getScopes called on multi-lfn %s",get());
    if (removeforeign)
        s += localpos;
    // num scopes = number of "::"s
    unsigned ret = 0;
    for (;;) {
        s = strstr(s,"::");
        if (!s)
            return ret;
        ret++;
        s += 2;
    }
}

StringBuffer &CDfsLogicalFileName::getScope(StringBuffer &buf,unsigned idx,bool removeforeign) const
{
    const char *s =lfn.get();
    if (!s)
        return buf;
    if (multi)
        DBGLOG("CDfsLogicalFileName::getScopes called on multi-lfn %s",get());
    if (removeforeign)
        s += localpos;
    // num scopes = number of "::"s
    for (;;) {
        const char *p = s;
        s = strstr(s,"::");
        if (idx--==0) {
            if (s)
                return buf.append(s-p,p);
            return buf.append(p);
        }
        if (!s)
            return buf;
        s += 2;
    }
}

StringBuffer &CDfsLogicalFileName::makeScopeQuery(StringBuffer &query, bool absolute) const
{
    if (multi)
        DBGLOG("CDfsLogicalFileName::makeFullnameQuery called on multi-lfn %s",get());
    if (absolute)
        query.append(SDS_DFS_ROOT "/");
    // returns full xpath for containing scope
    const char *s=get(true);    // skip foreign
    bool first=true;
    for (;;) {
        const char *e=strstr(s,"::");
        if (!e)
            break;
        if (first)
            first = false;
        else
            query.append('/');
        query.append("Scope[@name=\"").append(e-s,s).append("\"]");
        s = e+2;
    }
    return query;
}

StringBuffer &CDfsLogicalFileName::makeFullnameQuery(StringBuffer &query, DfsXmlBranchKind bkind, bool absolute) const
{
    makeScopeQuery(query,absolute);
    query.append('/').append(queryDfsXmlBranchName(bkind));
    return query.append("[@name=\"").append(queryTail()).append("\"]");
}

StringBuffer &CDfsLogicalFileName::makeXPathLName(StringBuffer &lfnNodeName) const
{
    const char *s=get(true);    // skip foreign
    // Ensure only chars that are accepted by jptree in an xpath element are used
    for (;;)
    {
        const char *e=strstr(s,"::");
        if ((e && 0 != strncmp(".", s, e-s)) || (!e && !streq(".", s))) // skip '.' scopes
        {
            lfnNodeName.append('_');
            while (s != e)
            {
                char c = *s;
                switch (c)
                {
                case '\0':
                    return lfnNodeName; // done
                case '^':
                    ++s;
                    if ('\0' == *s)
                        return lfnNodeName; // probably an error really to end in '^'
                    c = toupper(*s);
                    // fall through
                default:
                    if ('_' == c)
                        lfnNodeName.append("__"); // to avoid clash with use of '_' here was escape char.
                    else if (isValidXPathChr(c))
                        lfnNodeName.append(c);
                    else
                        lfnNodeName.append('_').append((unsigned) (unsigned char) c);
                    break;
                }
                ++s;
            }
        }
        if (!e)
            break;
        s = e+2;
    }
    return lfnNodeName;
}

bool CDfsLogicalFileName::getEp(SocketEndpoint &ep) const
{
    SocketEndpoint nullep;
    ep = nullep;
    const char *ns;
    if (isExternal())
    {
        const char * startPlane = skipScope(lfn, PLANE_SCOPE);
        if (startPlane)
        {
            const char * end = strstr(startPlane,"::");
            if (!end)
                return false;

            //Resolve the plane, and return the ip if it is a bare metal zone (or a legacy drop zone)
            StringBuffer planeName(end - startPlane, startPlane);
            Owned<IStoragePlane> plane = getDataStoragePlane(planeName, false);
            if (!plane)
                return false;

            const std::vector<std::string> &hosts = plane->queryHosts();
            if (hosts.size())
            {
                // assume first host ? Or should this throw an error? Or should there be a syntax to choosen Nth host in plane??
                ep.set(hosts[0].c_str());
            }
            else // mounted plane
                ep.set("localhost");
            return true;
        }
        ns = skipScope(lfn,EXTERNAL_SCOPE); // evaluates to null for a storage plane
    }
    else if (isForeign())
        ns = skipScope(lfn,FOREIGN_SCOPE);
    else
        ns = NULL;
    if (ns) {
        if (multi)
            DBGLOG("CDfsLogicalFileName::getEp called on multi-lfn %s",get());
        const char *e = strstr(ns,"::");
        if (e) {
            StringBuffer node(e-ns,ns);
            ep.set(node.str());
            return !ep.isNull();
        }
    }
    return false;
}

StringBuffer &CDfsLogicalFileName::getGroupName(StringBuffer &grp) const
{
    if (isExternal()) {
        const char *s = skipScope(lfn,EXTERNAL_SCOPE);
        if (s) {
            const char *e = strstr(s,"::");
            if (e)
                grp.append(e-s,s);
        }
        else
        {
            const char *s = skipScope(lfn,PLANE_SCOPE);
            if (s) {
                const char *e = strstr(s,"::");
                if (e)
                    grp.append(e-s,s);
            }
        }
    }
    return grp;
}

bool CDfsLogicalFileName::getExternalPath(StringBuffer &dir, StringBuffer &tail, bool iswin, IException **e) const
{
    if (e)
        *e = NULL;
    if (!isExternal()) {
        if (e)
            *e = MakeStringException(-1,"File not external (%s)",get());
        return false;
    }
    if (multi)
        DBGLOG("CDfsLogicalFileName::makeFullnameQuery called on multi-lfn %s",get());

    const char *s = skipScope(lfn,EXTERNAL_SCOPE);
    if (s)
    {
        //skip the ip address
        s = strstr(s,"::");
    }
    else
    {
        const char * startPlane = skipScope(lfn,PLANE_SCOPE);
        if (startPlane)
        {
            s = strstr(startPlane,"::");

            if (s)
            {
                StringBuffer planeName(s - startPlane, startPlane);
                Owned<IStoragePlane> plane = getDataStoragePlane(planeName, false);
                if (!plane)
                {
                    if (e)
                        *e = makeStringExceptionV(-1, "Scope contains unknown storage plane '%s'", planeName.str());
                    return false;
                }
                if (plane->numDevices() != 1)
                {
                    if (e)
                        *e = makeStringExceptionV(-1, "plane:: does not support planes with more than one device '%s'", planeName.str());
                    return false;
                }
                const char * prefix = plane->queryPrefix();
                //If the prefix is a PathSepChar, it should not be appended to the dir here because
                //a PathSepChar will be appended to the dir inside the expandExternalPath() if the s
                //is started with the "::".
                //Also a trailing pathsepchar in the prefix should be removed.
                if (!isRootDirectory(prefix))
                {
                    dir.append(prefix);
                    removeTrailingPathSepChar(dir);
                }
            }
        }
    }
    return expandExternalPath(dir, tail, get(), s, iswin, e);
}

bool CDfsLogicalFileName::getExternalFilename(RemoteFilename &rfn) const
{
    StringBuffer fn;
    if (!getExternalPath(fn,fn))
        return false;
    SocketEndpoint ep;
    getEp(ep);
    rfn.setPath(ep,fn.str());
    return !rfn.isNull();
}


bool CDfsLogicalFileName::setFromMask(const char *fname,const char *rootdir)
{
    if (!fname||!*fname)
        return false;
    // first remove base dir from fname if present
    DFD_OS os = SepCharBaseOs(getPathSepChar(fname));
    const char *dir = (rootdir&&*rootdir)?rootdir:queryBaseDirectory(grp_unknown, 0, os);
    // ignore drive if present
    if (os==DFD_OSwindows) {
        if (dir[1]==':')
            dir += 2;
        if (fname[1]==':')
            fname += 2;
    }
    else {
        if (dir[2]=='$')
            dir += 3;
        if (fname[2]=='$')
            fname += 3;
    }
    if (isPathSepChar(fname[0])) {
        while (*dir&&(*dir==*fname)) {
            dir++;
            fname++;
        }
        if (*dir||!isPathSepChar(*fname))
            return false; // didn't match base
        fname++;
    }
    StringBuffer logicalName;
    for (;;) {
        if (*fname==0)  // we didn't find tail
            return false;
        if (isPathSepChar(*fname))
            logicalName.append("::");
        else if (*fname=='.') {
            if (findPathSepChar(fname+1)==NULL) { // check for . mid name
                if (strchr(fname+1,'.')==NULL) { // check for multiple extension
                    fname++;
                    if (*fname=='_') {
                        for (;;) {
                            fname++;
                            if (!*fname)
                                return false;
                            if (memicmp(fname,"_of_",4)==0) {
                                if (!fname[4])
                                    return false;
                                return setValidate(logicalName.str());
                            }
                        }
                    }
                }
            }
            logicalName.append(*fname);
        }
        else
            logicalName.append((char)tolower(*fname));
        fname++;
    }
    return false;
}


const char * skipScope(const char *lname,const char *scope) // returns NULL if scope doesn't match
{
    // scope assumed normalized
    if (!scope||!*scope)
        return lname;
    if (!lname)
        return NULL;
    skipSp(lname);
    if (!*lname)
        return NULL;
    while (*scope) {
        if (toupper(*scope)!=toupper(*lname))
            return NULL;
        scope++;
        lname++;
    }
    skipSp(lname);
    if (*(lname++)!=':')
        return NULL;
    if (*(lname++)!=':')
        return NULL;
    skipSp(lname);
    return lname;
}

const char * querySdsFilesRoot()
{
    return SDS_DFS_ROOT;
}

const char * querySdsRelationshipsRoot()
{
    return SDS_RELATIONSHIPS_ROOT ;
}

#define PAF_HAS_SIZE (0x01)
#define PAF_HAS_DATE (0x02)
#define PAF_HAS_CRC  (0x04)
#define PAF_HAS_VAL  (0x08)
#define PAF_HAS_SUB  (0x10)
#define PAF_HAS_FILECRC (0x20)

MemoryBuffer &serializePartAttr(MemoryBuffer &mb,IPropertyTree *tree)
{
    byte flags = 0;
    offset_t size = (offset_t)tree->getPropInt64("@size",-1);
    if (size!=(offset_t)-1)
        flags |= PAF_HAS_SIZE;
    StringBuffer dts;
    CDateTime dt;
    if (tree->getProp("@modified",dts)&&dts.length()) {
        dt.setString(dts.str());
        if (!dt.isNull())
            flags |= PAF_HAS_DATE;
    }
    if (tree->hasProp("@fileCrc"))
        flags |= PAF_HAS_FILECRC;
    else if (tree->hasProp("@crc"))
        flags |= PAF_HAS_CRC;
    StringBuffer str;
    if (tree->getProp(NULL,str))
        flags |= PAF_HAS_VAL;
    if (tree->hasChildren())
        flags |= PAF_HAS_SUB;
    mb.append(flags);
    if (flags&PAF_HAS_SIZE)
        mb.append(size);
    if (flags&PAF_HAS_DATE)
        dt.serialize(mb);
    if (flags&PAF_HAS_FILECRC) {
        int crc = tree->getPropInt("@fileCrc");
        mb.append(crc);
    }
    else if (flags&PAF_HAS_CRC) {
        int crc = tree->getPropInt("@crc");
        mb.append(crc);
    }
    if (flags&PAF_HAS_VAL)
        mb.append(str.str());
    if (flags&PAF_HAS_SUB) {
        Owned<IPropertyTreeIterator> ci = tree->getElements("*");
        ForEach(*ci) {
            IPropertyTree &pt = ci->query();
            mb.append(pt.queryName());
            pt.serialize(mb);
        }
        mb.append(""); // chiild terminator. i.e. blank name.
    }
    Owned<IAttributeIterator> ai = tree->getAttributes();
    ForEach(*ai) {
        const char *name = ai->queryName();
        if (!name)
            continue;
        if (*name=='@')
            name++;
        if (strcmp(name,"size")==0)
            continue;
        if (strcmp(name,"modified")==0)
            continue;
        if (strcmp(name,"crc")==0)
            continue;
        if (strcmp(name,"fileCrc")==0)
            continue;
        if (strcmp(name,"num")==0)
            continue;
        mb.append(name);
        mb.append(ai->queryValue());
    }
    return mb.append(""); // attribute terminator. i.e. blank attr name.

}


IPropertyTree *deserializePartAttr(MemoryBuffer &mb)
{
    IPropertyTree *pt = createPTree("Part");
    byte flags;
    mb.read(flags);
    if (flags&PAF_HAS_SIZE) {
        offset_t size;
        mb.read(size);
        pt->setPropInt64("@size",size);
    }
    if (flags&PAF_HAS_DATE) {
        CDateTime dt;
        dt.deserialize(mb);
        StringBuffer dts;
        pt->setProp("@modified",dt.getString(dts).str());
    }
    if (flags&PAF_HAS_FILECRC) {
        int crc;
        mb.read(crc);
        pt->setPropInt("@fileCrc",crc);
    }
    else if (flags&PAF_HAS_CRC) {
        int crc;
        mb.read(crc);
        pt->setPropInt("@crc",crc);
    }
    if (flags&PAF_HAS_VAL) {
        StringAttr val;
        mb.read(val);
        pt->setProp(NULL, val);
    }
    if (flags&PAF_HAS_SUB) {
        for (;;) {
            StringAttr name;
            mb.read(name);
            if (name.length()==0)
                break;
            pt->addPropTree(name.get(),createPTree(mb));
        }
    }
    StringAttr _aname;
    StringAttr avalue;
    StringBuffer aname("@");
    for (;;) {
        mb.read(_aname);
        if (!_aname.length())
            break;
        mb.read(avalue);
        if (_aname[0]=='@')
            pt->setProp(_aname, avalue);
        else  {
            aname.setLength(1);
            aname.append(_aname);
            pt->setProp(aname.str(), avalue);
        }
    }
    return pt;
}




IPropertyTreeIterator *deserializePartAttrIterator(MemoryBuffer &mb)    // takes ownership of mb
{
    class cPartAttrIterator: implements IPropertyTreeIterator, public CInterface
    {
        Owned<IPropertyTree> cur;
        unsigned pn;
    public:
        IMPLEMENT_IINTERFACE;
        MemoryBuffer mb;

        bool first()
        {
            mb.reset();
            pn = 0;
            return next();
        }

        bool next()
        {
            cur.clear();
            pn++;
            if (mb.getPos()>=mb.length())
                return false;
            cur.setown(deserializePartAttr(mb));
            cur->setPropInt("@num",pn);
            return true;
        }

        bool isValid()
        {
            return cur.get()!=NULL;
        }

        IPropertyTree  & query()
        {
            return *cur;
        }


    } *pai = new cPartAttrIterator;
    mb.swapWith(pai->mb);
    return pai;
}

unsigned getFileGroups(const char *grplist,StringArray &groups)
{
    if (!grplist)
        return 0;
    const char *s = grplist;
    StringBuffer gs;
    unsigned sq = 0;
    unsigned pa = 0;
    for (;;) {
        char c = *(s++);
        if (!c||((c==',')&&!sq&&!pa)) {
            gs.clip();
            if (gs.length()) {
                if (strcmp(gs.str(),"SuperFiles")!=0) // special case kludge
                    gs.toLowerCase();
                bool add = true;
                ForEachItemIn(j,groups) {
                    if (strcmp(groups.item(j),gs.str())==0) {
                        add = false;
                        break;
                    }
                }
                if (add)
                    groups.append(gs.str());
                gs.clear();
            }
            if (!c)
                break;
        }
        else {
            if (c=='[')
                sq++;
            else if (sq&&(c==']'))
                sq--;
            else if (c=='(')            // future expansion
                pa++;
            else if (pa&&(c==')'))
                pa--;
            gs.append(c);
        }
    }
    return groups.ordinality();
}

unsigned getFileGroups(IPropertyTree *pt,StringArray &groups, bool checkclusters)
{
    unsigned ret = 0;
    if (pt) {
        ret = getFileGroups(pt->queryProp("@group"),groups);
        if (ret==0) {
            if (pt->getPropInt("@numparts")==1) {
                const char *g = pt->queryProp("Part[@num=\"1\"][1]/@node");
                if (g&&*g) {
                    groups.append(g);
                    return 1;
                }
            }
        }
        if (checkclusters) {
            const char * on = pt->queryProp("OrigName");
            if (!on)
                on = "<UNKNOWN>";
            unsigned nc = pt->getPropInt("@numclusters");
            if (nc&&(nc!=groups.ordinality())) {
                OERRLOG("%s groups/numclusters mismatch",on);
            }
            MemoryAttr ma;
            unsigned ng = groups.ordinality();
            bool *found = (bool *)ma.allocate(sizeof(bool)*ng);
            memset(found,0,sizeof(bool)*ng);
            Owned<IPropertyTreeIterator> iter = pt->getElements("Cluster");
            bool anyfound = false;
            ForEach(*iter) {
                StringBuffer clabel;
                const char *cname = iter->query().queryProp("@roxiePrefix");
                if (!cname||!*cname)
                    cname = iter->query().queryProp("@name");
                if (!cname&&*cname) {
                    continue;
                }
                anyfound = true;
                bool ok = false;
                ForEachItemIn(i,groups) {
                    if (strcmp(cname,groups.item(i))==0) {
                        if (found[i]) {
                            OERRLOG("'%s' has duplicate cluster",on);
                        }
                        else
                            found[i] = true;
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    const char * gs = pt->queryProp("@group");
                    OERRLOG("'%s' has missing cluster(%s) in groups(%s)",on,cname,gs?gs:"NULL");
                }
            }
            if (anyfound) {
                for (unsigned i=0;i<ng;i++)
                    if (!found[i])
                        DBGLOG("'%s' has missing group(%s) in clusters",on,groups.item(i));
            }
        }
    }
    return ret;
}


bool shrinkFileTree(IPropertyTree *file)
{
    if (!file)
        return false;
    if (file->hasProp("Parts"))
        return true;
    unsigned n = file->getPropInt("@numparts",0);
    if (n<2)
        return true;
    const char *group=file->queryProp("@group");
    if (!group||!*group||!file->hasProp("Part[2]")) // don't shrink single part files
        return true;
    MemoryBuffer mb;
    IPropertyTree **parts = (IPropertyTree **)calloc(n,sizeof(IPropertyTree *));

    unsigned i;
    for (;;) {
        IPropertyTree *part = file->getBranch("Part[1]");
        if (!part)
            break;
        file->removeTree(part);
        i = part->getPropInt("@num",0)-1;
        if ((i<n)&&(parts[i]==NULL))
            parts[i] = part;
        else
            part->Release();
    }
    for (i=0;i<n;i++) {
        if (!parts[i])
            parts[i] = createPTree("Part");
        serializePartAttr(mb,parts[i]);
    }
    file->setPropBin("Parts",mb.length(),mb.toByteArray());
    for (i=0;i<n;i++)
        parts[i]->Release();
    free(parts);
    return true;
}

void expandFileTree(IPropertyTree *file,bool expandnodes,const char *cluster)
{
    if (!file)
        return;
    MemoryBuffer mb;
    if (file->getPropBin("Parts",mb)) {
        file->removeProp("Parts");
        Owned<IPropertyTreeIterator> pi = deserializePartAttrIterator(mb);
        ForEach(*pi)
            file->addPropTree("Part",&pi->get());
    }
    if (!expandnodes)
        return;
    StringArray groups;
    unsigned ng = getFileGroups(file,groups);
    unsigned cn = 0;
    if (cluster&&*cluster) {
        unsigned i;
        for (i=0;i<ng;i++)
            if (strcmp(cluster,groups.item(i))==0) {
                cn = i;
                break;
            }
        if (i==ng)
            OERRLOG("expandFileTree: Cluster %s not found in file",cluster);
    }
    if (cn<ng) {
        const char *gname = groups.item(cn);
        Owned<IClusterInfo> clusterinfo;
        Owned<IPropertyTreeIterator> iter = file->getElements("Cluster");
        ForEach(*iter) {
            clusterinfo.setown(deserializeClusterInfo(&iter->query(),&queryNamedGroupStore(),0));
            StringBuffer clabel;
            const char *cname = clusterinfo->getClusterLabel(clabel).str();
            if (cname&&(strcmp(cname,gname)==0))
                break;
        }
        file->setProp("@group",gname);
        Owned<IGroup> grp;
        if (!clusterinfo) // try to resolve using cluster info (e.g. when remote)
            grp.setown(queryNamedGroupStore().lookup(gname)); // resolve locally (legacy)
        if (grp||clusterinfo) {
            BoolArray done;
            Owned<IPropertyTreeIterator> iter = file->getElements("Part");
            unsigned max = file->getPropInt("@numparts");
            StringBuffer ips;
            ForEach(*iter) {
                unsigned num = iter->query().getPropInt("@num");
                if (num--) {
                    while (num>=done.ordinality())
                        done.append(false);
                    if (!done.item(num)) {
                        done.replace(true,num);
                        INode *node = clusterinfo?clusterinfo->queryNode(num,max,0):&grp->queryNode(num%grp->ordinality());
                        if (node) {
                            node->endpoint().getHostText(ips.clear());
                            iter->query().setProp("@node",ips.str());
                        }
                    }
                }
            }
        }
        if (clusterinfo&&!file->hasProp("@replicated")) // legacy
            file->setPropBool("@replicated",clusterinfo->queryPartDiskMapping().isReplicated());

    }
}

void filterParts(IPropertyTree *file,UnsignedArray &partslist)
{
    if (!file)
        return;
    unsigned max = file->getPropInt("@numparts");
    StringBuffer xpath;
    for (unsigned i=0;i<max;i++) {
        bool del = true;
        ForEachItemIn(j,partslist) {
            if (i==partslist.item(j)) {
                del = false;
                break;
            }
        }
        if (del) {
            xpath.clear().append("Part[@num=\"").append(i+1).append("\"]");
            Owned<IPropertyTree> child = file->getPropTree(xpath.str());
            file->removeTree(child);
        }
    }
    
}


//===================================================================

#define SDS_LOCK_TIMEOUT (1000*60*5)
#define SORT_REVERSE 1
#define SORT_NOCASE  2
#define SORT_NUMERIC 4
#define SORT_FLOAT   8

void ensureSDSPath(const char * sdsPath)
{
    if (!sdsPath)
        throw MakeStringException(-1,"Attempted to create empty DALI path");

    Owned<IRemoteConnection> conn = querySDS().connect(sdsPath, myProcessSession(), RTM_LOCK_WRITE | RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
    if (!conn)
        throw MakeStringException(-1,"Could not create DALI %s branch",sdsPath);
}

inline void filteredAdd(IArrayOf<IPropertyTree> &results,const char *namefilterlo,const char *namefilterhi,StringArray& unknownAttributes, IPropertyTree *item)
{

    if (!item)
        return;
    if (namefilterlo||namefilterhi) {
        const char *n = item->queryName();
        if (!n)
            return;
        if (namefilterlo&&(strcmp(namefilterlo,n)>0))
            return;
        if (namefilterhi&&(strcmp(namefilterhi,n)<0))
            return;
    }
    ForEachItemIn(i, unknownAttributes) {
        const char *attribute = unknownAttributes.item(i);
        if (!attribute || !*attribute)
            continue;
        const char *attrValue = item->queryProp(attribute);
        if (attrValue && *attrValue)
            return;
    }
    item->Link();
    results.append(*item);
}


class cSort
{
    static CriticalSection sortsect;
    static cSort *sortthis;
    mutable CLargeMemoryAllocator buf;
    CIArrayOf<CIStringArray> sortKeys;
    IArrayOf<IPropertyTree> sortvalues;
    IntArray modifiers;
    mutable char **vals;
    unsigned nv;
    unsigned nk;
public:
    cSort()
        : buf(0x100000*100,0x10000,true)
    {
    }
    void dosort(IPropertyTreeIterator &iter,const char *sortorder, const char *namefilterlo,const char *namefilterhi, StringArray& unknownAttributes, IArrayOf<IPropertyTree> &results)
    {
        StringBuffer sk;
        const char *s = sortorder;
        int mod = 0;
        for (;;) {
            if (!*s||(*s==',')) {
                if (sk.length()) {
                    // could add '-' and '?' prefixes here (reverse/caseinsensitive)
                    Owned<CIStringArray> keyList = new CIStringArray;
                    keyList->appendListUniq(sk.str(), "|");
                    sortKeys.append(*keyList.getClear());
                    modifiers.append(mod);
                    sk.clear();
                }
                mod = 0;
                if (!*s)
                    break;
            }
            else if ((*s=='-')&&(sk.length()==0))
                mod |= SORT_REVERSE;
            else if ((*s=='?')&&(sk.length()==0))
                mod |= SORT_NOCASE;
            else if ((*s=='#')&&(sk.length()==0))
                mod |= SORT_NUMERIC;
            else if ((*s=='~')&&(sk.length()==0))
                mod |= SORT_FLOAT;
            else
                sk.append(*s);
            s++;
        }
        ForEach(iter)
            filteredAdd(sortvalues,namefilterlo,namefilterhi,unknownAttributes,&iter.query());
        nv = sortvalues.ordinality();
        nk = sortKeys.ordinality();
        vals = (char **)calloc(sizeof(char *),nv*nk);
        unsigned *idx=(unsigned *)malloc(sizeof(unsigned)*nv);
        unsigned i;
        for (i=0;i<nv;i++)
            idx[i] = i;
        {
            CriticalBlock block(sortsect);
            sortthis = this;
            qsort(idx,nv,sizeof(unsigned),compare);
        }
        for (i=0;i<nv;i++) {
            IPropertyTree &item = sortvalues.item((unsigned)idx[i]);
            item.Link();
            results.append(item);
        }
        free(vals);
        free(idx);
    }

    void getkeyval(unsigned idx,unsigned k,char *&val) const
    {
        StringArray &keys = sortKeys.item(k);
        const char *key = keys.item(0); // must be >=1
        const char *v;
        if ((key[0]=='@')&&(key[1]==0))
        {
            v = sortvalues.item(idx).queryName();
            dbgassertex(1 == keys.ordinality()); // meaningless for multivalue key if key is special key "@"
        }
        else
        {
            unsigned k2=1;
            for (;;)
            {
                v = sortvalues.item(idx).queryProp(key);
                if (v || k2 == keys.ordinality())
                    break;
                key = keys.item(k2++);
            }
        }
        if (!v)
            v = "";
        size32_t l = strlen(v)+1;
        val = (char *)buf.alloc(l);
        memcpy(val,v,l);
    }

    int docompare(unsigned i1,unsigned i2) const
    {
        if (i1!=i2) {
            for (unsigned i=0;i<nk;i++) {
                int mod = modifiers.item(i);
                char *&v1 = vals[i1*nk+i];
                if (!v1)
                    getkeyval(i1,i,v1);
                char *&v2 = vals[i2*nk+i];
                if (!v2)
                    getkeyval(i2,i,v2);
                if (!v1 || !v2)
                    return 0;
                int ret;
                if (mod&SORT_NUMERIC)
                {
                    __int64 ret0 = _atoi64(v1)-_atoi64(v2);
                    if (ret0 > 0)
                        ret = 1;
                    else if (ret0 < 0)
                        ret = -1;
                    else
                        ret = 0;
                }
                else if (mod&SORT_FLOAT)
                {
                    double ret0 = atof(v1) - atof(v2);
                    if (ret0 > 0)
                        ret = 1;
                    else if (ret0 < 0)
                        ret = -1;
                    else
                        ret = 0;
                }
                else if (mod&SORT_NOCASE)
                    ret = stricmp(v1,v2);
                else
                    ret = strcmp(v1,v2);
                if (ret) {
                    if (mod&SORT_REVERSE)
                        ret = -ret;
                    return ret;
                }
            }
        }
        return 0;
    }

    static int compare(const void *a,const void *b)
    {
        return sortthis->docompare(*(unsigned*)a,*(unsigned*)b);
    }

};

CriticalSection cSort::sortsect;
cSort *cSort::sortthis;


IRemoteConnection *getSortedElements( const char *basexpath,
                                     const char *xpath,
                                     const char *sortorder,
                                     const char *namefilterlo,
                                     const char *namefilterhi,
                                     StringArray& unknownAttributes,
                                     IArrayOf<IPropertyTree> &results)
{
    Owned<IRemoteConnection> conn = querySDS().connect(basexpath, myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    Owned<IPropertyTreeIterator> iter = conn->getElements(xpath);
    if (!iter)
        return NULL;

    sortElements(iter, sortorder, namefilterlo,namefilterhi,unknownAttributes, results);
    return conn.getClear();
}


//==================================================================================

#define PAGE_CACHE_TIMEOUT (1000*60*10)
#define MAX_PAGE_CACHE_ITEMS 1000
static unsigned pageCacheTimeoutMilliSeconds = PAGE_CACHE_TIMEOUT;
void setPageCacheTimeoutMilliSeconds(unsigned timeoutSeconds)
{
    pageCacheTimeoutMilliSeconds = 1000 * timeoutSeconds;
}

static unsigned maxPageCacheItems = MAX_PAGE_CACHE_ITEMS;
void setMaxPageCacheItems(unsigned _maxPageCacheItems)
{
    maxPageCacheItems = _maxPageCacheItems;
}

class CTimedCacheItem: public CInterface
{
protected: friend class CTimedCache;
    unsigned timestamp = 0;
    StringAttr owner;
public:
    DALI_UID hint;
    CTimedCacheItem(const char *_owner)
        : owner(_owner)
    {
        hint = queryCoven().getUniqueId();
    }
};

class CTimedCache
{

    class cThread: public Thread
    {
    public:
        CTimedCache *parent;
        cThread()
            : Thread("CTimedCache::thread")
        {
        }

        int run()
        {
            parent->run();
            return 0;
        }
    } thread;

    unsigned check()
    {
        /* The items are ordered, such that oldest items are at the start.
        This method scans through from oldest to newest until the current
        item's "due" time has not expired. It then removes all up to that
        point, i.e. those that have expired, and returns the timing 
        difference between now and the next due time. */
        unsigned expired = 0;
        unsigned res = (unsigned)-1;
        unsigned now = msTick();
        ForEachItemIn(i, items)
        {
            CTimedCacheItem &item = items.item(i);
            if (now - item.timestamp < pageCacheTimeoutMilliSeconds)
            {
                res = pageCacheTimeoutMilliSeconds - (now - item.timestamp);
                break;
            }
            expired++;
        }
        if (expired > 0)
            items.removen(0, expired);
        return res;
    }


public:
    void run()
    {
        CriticalBlock block(sect);
        while(!stopping) {
            unsigned delay=check();
            CriticalUnblock unblock(sect);
            sem.wait(delay);
        }
    }



    CIArrayOf<CTimedCacheItem> items;
    CriticalSection sect;
    Semaphore sem;
    bool stopping;

    CTimedCache()
    {
        stopping = false;
        thread.parent = this;
    }

    ~CTimedCache()
    {
        stop();
    }


    DALI_UID add(CTimedCacheItem *item)
    {
        if (!item)
            return 0;
        CriticalBlock block(sect);
        if ((maxPageCacheItems > 0) && (maxPageCacheItems == items.length()))
            items.remove(0);
        item->timestamp = msTick();
        items.append(*item);
        DALI_UID ret = item->hint;
        sem.signal();
        return ret;
    }

    void remove(CTimedCacheItem *item)
    {
        CriticalBlock block(sect);
        items.zap(*item);
    }

    CTimedCacheItem *get(const char *owner,DALI_UID hint)
    {
        CriticalBlock block(sect);
        ForEachItemInRev(i,items) {
            CTimedCacheItem &item = items.item(i);
            if ((item.hint==hint)&&(strcmp(item.owner,owner)==0)) {
                item.Link();
                items.remove(i);
                return &item;
            }
        }
        return NULL;
    }

    void start()
    {
        thread.start(false);
    }

    void stop()
    {
        {
            CriticalBlock block(sect);
            stopping = true;
            sem.signal();
        }
        thread.join();
    }
};

static CriticalSection pagedElementsCacheSect;
static CTimedCache *pagedElementsCache=NULL;


class CPECacheElem: public CTimedCacheItem
{
public:
    CPECacheElem(const char *owner, ISortedElementsTreeFilter *_postFilter)
        : CTimedCacheItem(owner), postFilter(_postFilter), postFiltered(0)
    {
        passesFilter.setown(createThreadSafeBitSet());
    }
    ~CPECacheElem()
    {
    }
    Owned<IRemoteConnection> conn;
    IArrayOf<IPropertyTree> totalres;
    Linked<ISortedElementsTreeFilter> postFilter;
    unsigned postFiltered;
    Owned<IBitSet> passesFilter;
};

void sortElements(IPropertyTreeIterator* elementsIter,
                    const char *sortOrder,
                    const char *nameFilterLo,
                    const char *nameFilterHi,
                    StringArray& unknownAttributes,
                    IArrayOf<IPropertyTree> &sortedElements)
{
    if (nameFilterLo&&!*nameFilterLo)
        nameFilterLo = NULL;
    if (nameFilterHi&&!*nameFilterHi)
        nameFilterHi = NULL;
    if (sortOrder && *sortOrder)
    {
        cSort sort;
        sort.dosort(*elementsIter,sortOrder,nameFilterLo,nameFilterHi,unknownAttributes, sortedElements);
    }
    else
        ForEach(*elementsIter)
            filteredAdd(sortedElements,nameFilterLo,nameFilterHi,unknownAttributes, &elementsIter->query());
};

IRemoteConnection *getElementsPaged( IElementsPager *elementsPager,
                                     unsigned startoffset,
                                     unsigned pagesize,
                                     ISortedElementsTreeFilter *postfilter, // filters before adding to page
                                     const char *owner,
                                     __int64 *hint,
                                     IArrayOf<IPropertyTree> &results,
                                     unsigned *total,
                                     bool *allMatchingElementsReceived,
                                     bool checkConn)
{
    if ((pagesize==0) || !elementsPager)
        return NULL;
    if (maxPageCacheItems > 0)
    {
        CriticalBlock block(pagedElementsCacheSect);
        if (!pagedElementsCache)
        {
            pagedElementsCache = new CTimedCache;
            pagedElementsCache->start();
        }
    }
    Owned<CPECacheElem> elem;
    if (hint && *hint && (maxPageCacheItems > 0))
    {
        elem.setown(QUERYINTERFACE(pagedElementsCache->get(owner,*hint),CPECacheElem)); // NB: removes from cache in process, added back at end
        if (elem)
            postfilter = elem->postFilter; // reuse cached postfilter
    }
    if (!elem)
    {
        elem.setown(new CPECacheElem(owner, postfilter));
        elem->conn.setown(elementsPager->getElements(elem->totalres));
    }
    if (checkConn && !elem->conn)
        return NULL;
    unsigned n;
    if (total)
        *total = elem->totalres.ordinality();
    if (postfilter) {
        unsigned numFiltered = 0;
        n = 0;
        ForEachItemIn(i,elem->totalres) {
            IPropertyTree &item = elem->totalres.item(i);
            bool passesFilter = false;
            if (elem->postFiltered>i) // postFiltered is high water mark of items checked
                passesFilter = elem->passesFilter->test(i);
            else
            {
                passesFilter = postfilter->isOK(item);
                elem->passesFilter->set(i, passesFilter);
                elem->postFiltered = i+1;
            }
            if (passesFilter)
            {
                if (n>=startoffset) {
                    item.Link();
                    results.append(item);
                    if (results.ordinality()>=pagesize)
                    {
                        // if total needed, need to iterate through all items
                        if (NULL == total)
                            break;
                        startoffset = (unsigned)-1; // no more results needed
                    }
                }
                n++;
            }
            else
                ++numFiltered;
        }
        if (total)
            *total -= numFiltered;
    }
    else {
        n = (elem->totalres.ordinality()>startoffset)?(elem->totalres.ordinality()-startoffset):0;
        if (n>pagesize)
            n = pagesize;
        for (unsigned i=startoffset;i<startoffset+n;i++) {
            IPropertyTree &item = elem->totalres.item(i);
            item.Link();
            results.append(item);
        }
    }
    if (allMatchingElementsReceived)
        *allMatchingElementsReceived = elementsPager->allMatchingElementsReceived();
    IRemoteConnection *ret = NULL;
    if (elem->conn)
        ret = elem->conn.getLink();
    if (hint && (maxPageCacheItems > 0))
    {
        *hint = elem->hint;
        pagedElementsCache->add(elem.getClear());
    }
    return ret;
}

void clearPagedElementsCache()
{
    CriticalBlock block(pagedElementsCacheSect);
    try {
        delete pagedElementsCache;
    }
    catch (IMP_Exception *e)
    {
        if (e->errorCode()!=MPERR_link_closed)
            throw;
        e->Release();
    }
    catch (IDaliClient_Exception *e) {
        if (e->errorCode()!=DCERR_server_closed)
            throw;
        e->Release();
    }
    catch (IException *e)
    {
        EXCLOG(e, "clearPagedElementsCache");
        e->Release();
    }
    pagedElementsCache = NULL;
}


void CSDSFileScanner::processScopes(IRemoteConnection *conn,IPropertyTree &root,StringBuffer &name)
{
    if (!checkScopeOk(name))
        return;
    size32_t ns = name.length();
    if (ns)
        name.append("::");
    size32_t ns2 = name.length();

    Owned<IPropertyTreeIterator> iter = root.getElements("Scope");
    ForEach(*iter) {
        IPropertyTree &scope = iter->query();
        const char *sn = scope.queryProp("@name");
        if (!sn||!*sn)
            continue;
        name.append(sn);
        processScopes(conn,scope,name);
        name.setLength(ns2);
        conn->rollbackChildren(&scope,true);
    }
    processFiles(conn,root,name);
    name.setLength(ns);

}

void CSDSFileScanner::processFiles(IRemoteConnection *conn,IPropertyTree &root,StringBuffer &name)
{
    size32_t ns = name.length();
    if (includefiles) {
        Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_File));
        ForEach(*iter) {
            IPropertyTree &file = iter->query();
            const char *fn = file.queryProp("@name");
            if (!fn||!*fn)
                continue;
            name.append(fn);
            if (checkFileOk(file,name.str())) {
                processFile(file,name);
            }
            else
                ; // DBGLOG("ignoreFile %s",name.str());

            name.setLength(ns);
            conn->rollbackChildren(&file,true);
        }
    }
    if (includesuper) {
        Owned<IPropertyTreeIterator> iter = root.getElements(queryDfsXmlBranchName(DXB_SuperFile));
        ForEach(*iter) {
            IPropertyTree &file = iter->query();
            const char *fn = file.queryProp("@name");
            if (!fn||!*fn)
                continue;
            name.append(fn);
            if (checkSuperFileOk(file,name.str())) {
                file.getBranch(NULL);
                processSuperFile(file,name);
            }
            name.setLength(ns);
            conn->rollbackChildren(&file,true);
        }
    }
}

void CSDSFileScanner::scan(IRemoteConnection *conn,
                           bool _includefiles,
                           bool _includesuper)
{
    includefiles = _includefiles;
    includesuper = _includesuper;
    StringBuffer name;
    Owned<IPropertyTree> root=conn->getRoot();
    processScopes(conn,*root,name);
}

bool CSDSFileScanner::singlefile(IRemoteConnection *conn,CDfsLogicalFileName &lfn)
{
    if (!conn)
        return false;
    Owned<IPropertyTree> root=conn->getRoot();
    if (!lfn.isSet())
        return false;
    StringBuffer query;
    lfn.makeFullnameQuery(query,DXB_File,false);
    IPropertyTree *file = root->queryPropTree(query.str());
    StringBuffer name;
    lfn.get(name);
    if (file) {
        if (checkFileOk(*file,name.str())) {
            processFile(*file,name);
        }
    }
    else { // try super
        lfn.makeFullnameQuery(query.clear(),DXB_SuperFile,false);
        file = root->queryPropTree(query.str());
        if (!file)
            return false;
        if (checkSuperFileOk(*file,name.str())) {
            processSuperFile(*file,name);
        }
    }
    return true;
}

extern da_decl bool isAnonCluster(const char *grp)
{
    if (!grp)
        return false;
    return((memicmp(grp,"__cluster",9)==0)||(memicmp(grp,"__anon",6)==0));
}

IClusterFileScanIterator *getClusterFileScanIterator(
                      IRemoteConnection *conn,  // conn is connection to Files
                      IGroup *group,        // only scans file with nodes in specified group
                      bool exactmatch,          // only files that match group exactly
                      bool anymatch,            // any nodes match
                      bool loadbranch,
                      IUserDescriptor *user)
{
    class cFileScanIterator: implements IClusterFileScanIterator, public CInterface
    {
        Owned<IPropertyTree> cur;
        unsigned fn;
        Linked<IRemoteConnection> conn;
        Linked<IGroup> lgrp;
        bool loadbranch;
        bool exactmatch;
        bool anymatch;
    public:
        StringArray filenames;

        IMPLEMENT_IINTERFACE;

        cFileScanIterator(IRemoteConnection *_conn,bool _loadbranch, IGroup *_lgrp, bool _exactmatch, bool _anymatch)
            : conn(_conn), lgrp(_lgrp)
        {
            fn = 0;
            loadbranch = _loadbranch;
            exactmatch = _exactmatch;
            anymatch = _anymatch;
        }

        bool first()
        {
            fn = 0;
            if (!conn)
                return false;
            return next();
        }

        bool next()
        {
            cur.clear();
            for (;;) {
                if (fn>=filenames.ordinality())
                    return false;
                const char *fns = filenames.item(fn++);
                bool needcheck = false;
                if (fns[0]=='\n') { // need to check
                    needcheck = true;
                    fns++;
                }
                CDfsLogicalFileName lfn;
                lfn.set(fns);
                StringBuffer query;
                lfn.makeFullnameQuery(query,DXB_File,false);
                if (loadbranch)
                    cur.setown(conn->queryRoot()->getBranch(query.str()));
                else
                    cur.setown(conn->queryRoot()->getPropTree(query.str()));
                if (needcheck) {
                    Owned<IFileDescriptor> fdesc = deserializeFileDescriptorTree(cur);
                    bool ok = false;
                    Owned<IGroup> group = fdesc->getGroup();
                    GroupRelation cmp = group->compare(lgrp);
                    if (group.get()) {
                        if ((cmp==GRidentical)||
                            (!exactmatch&&((cmp==GRbasesubset)||(cmp==GRwrappedsuperset)))||
                            (anymatch&&(cmp!=GRdisjoint)))
                        {
                            ok = true;
                            break;
                        }
                    }
                    if (!ok)
                        cur.clear();
                }
            } while (!cur.get());
            return true;
        }

        bool isValid()
        {
            return cur.get()!=NULL;
        }

        IPropertyTree  & query()
        {
            return *cur;
        }

        const char *queryName()
        {
            if (fn&&(fn<=filenames.ordinality()))
                return filenames.item(fn-1);    // fn always kept +1
            return NULL;
        }


    } *ret = new cFileScanIterator(conn,loadbranch,group,exactmatch,anymatch);
    StringArray candidategroups;
    if (group) {
        Owned<INamedGroupIterator> iter = queryNamedGroupStore().getIterator();
        StringBuffer name;
        ForEach(*iter) {
            iter->get(name.clear());
            Owned<IGroup> lgrp = queryNamedGroupStore().lookup(name.str());
            GroupRelation cmp = group->compare(lgrp);
            if ((cmp==GRidentical)||
                (!exactmatch&&((cmp==GRbasesubset)||(cmp==GRwrappedsuperset)))||
                (anymatch&&(cmp!=GRdisjoint)))
            {
                candidategroups.append(name.str());
            }
        }
    }
    Owned<IDFAttributesIterator> iter=queryDistributedFileDirectory().getDFAttributesIterator("*",user);
    StringBuffer chkname;
    StringBuffer gname;
    ForEach(*iter) {
        IPropertyTree &attr = iter->query();
        const char *name = attr.queryProp("@name");
        if (!name||!*name)
            continue;
        if (!group) {
            ret->filenames.append(name);
            continue;
        }
        if (exactmatch && (attr.getPropInt("@numparts")!=group->ordinality()))
            continue;
        StringArray groups;
        if (getFileGroups(&attr,groups)==0) {
            StringBuffer chkname;
            chkname.clear().append('\n').append(name);  // indicates needs checking
            ret->filenames.append(chkname.str());
            continue;
        }
        bool matched = false;
        ForEachItemIn(i,groups) {
            ForEachItemIn(j,candidategroups) {
                if (stricmp(groups.item(i),candidategroups.item(j))==0) {
                    matched = true;
                    break;
                }
            }
            if (matched)
                break;
        }
    }
    return ret;
}

typedef MapStringTo<bool> IsSuperFileMap;

void getLogicalFileSuperSubList(MemoryBuffer &mb, IUserDescriptor *user)
{
    // for fileservices
    IsSuperFileMap supermap;
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",user,true,true);
    if (iter) {
        ForEach(*iter) {
            IPropertyTree &attr=iter->query();
            if (attr.hasProp("@numsubfiles")) {
                const char *name=attr.queryProp("@name");
                if (name&&*name) {
                    if (!supermap.getValue(name))
                        supermap.setValue(name, true);
                }
            }
        }
    }

    HashIterator siter(supermap);
    ForEach(siter) {
        const char *supername = (const char *)siter.query().getKey();
        CDfsLogicalFileName lname;
        lname.set(supername);
        StringBuffer query;
        lname.makeFullnameQuery(query, DXB_SuperFile, true);
        Owned<IRemoteConnection> conn = querySDS().connect(query.str(),myProcessSession(),0, INFINITE);
        if (conn) { // Superfile may have disappeared by this stage, ignore.
            Owned<IPropertyTree> root = conn->getRoot();
            unsigned n=root->getPropInt("@numsubfiles");
            StringBuffer path;
            StringBuffer subname;
            unsigned subnum = 0;
            for (unsigned si=0;si<n;si++) {
                IPropertyTree *sub = root->queryPropTree(path.clear().appendf("SubFile[@num=\"%d\"]",si+1).str());
                if (sub) {
                    const char *subname = sub->queryProp("@name");
                    if (subname&&*subname) {
                        if (!supermap.getValue(subname)) {
                            size32_t sz = strlen(supername);
                            mb.append(sz).append(sz,supername);
                            sz = strlen(subname);
                            mb.append(sz).append(sz,subname);
                        }
                    }
                }
            }
        }
    }
}




class cDaliMutexSub: implements ISDSSubscription, public CInterface
{
    Semaphore &sem;
public:
    IMPLEMENT_IINTERFACE;
    cDaliMutexSub(Semaphore &_sem)
        : sem(_sem)
    {
    }
    virtual void notify(SubscriptionId id, const char *xpath, SDSNotifyFlags flags, unsigned valueLen, const void *valueData)
    {
//      DBGLOG("Notification(%" I64F "x) of %s - flags = %d",(__int64) id, xpath, flags);
        sem.signal();
    }
};


class CDaliMutex: implements IDaliMutex, public CInterface
{
    StringAttr name;
    CriticalSection crit;
    Semaphore sem;
    unsigned count;
    Owned<IRemoteConnection> oconn;
    SessionId mysession;
    bool stopping;

public:
    IMPLEMENT_IINTERFACE;
    CDaliMutex(const char *_name)
        : name(_name)
    {
        count = 0;
        mysession = myProcessSession();
        stopping = false;
    }


    bool enter(unsigned timeout=(unsigned)-1,IDaliMutexNotifyWaiting *notify=NULL)
    {
        CriticalBlock block(crit);      // crit is held if blocked
        if (count++)
            return true;


        Owned<IRemoteConnection> conn;
        Owned<cDaliMutexSub> sub = new cDaliMutexSub(sem);
        CTimeMon tm(timeout);
        bool first = true;
        bool needstop = false;
        StringBuffer path;
        StringBuffer opath;
        while (!stopping) {
            // first lock semaphore to write
            path.setf("/Locks/Mutex[@name=\"%s\"]",name.get());
            conn.setown(querySDS().connect(path.str(),mysession,RTM_LOCK_WRITE,SDS_CONNECT_TIMEOUT));
            if (conn) {
                SessionId oid = (SessionId)conn->queryRoot()->getPropInt64("Owner",0);
                if (!oid||querySessionManager().sessionStopped(oid,0)) {
                    opath.set(path);
                    opath.append("/Owner");
                    oconn.setown(querySDS().connect(opath.str(),mysession,RTM_CREATE|RTM_LOCK_WRITE|RTM_DELETE_ON_DISCONNECT,SDS_CONNECT_TIMEOUT));
                    if (!oconn)
                        throw MakeStringException(-1,"CDaliMutex::enter Cannot create %s branch",opath.str());
                    oconn->queryRoot()->setPropInt64(NULL,mysession);
                    oconn->commit();
                    if (needstop)
                        notify->stopWait(true);
                    return true;
                }
            }
            else {
                Owned<IRemoteConnection> pconn = querySDS().connect("Locks",mysession,RTM_LOCK_WRITE|RTM_CREATE_QUERY,SDS_CONNECT_TIMEOUT);
                if (!pconn)
                    throw MakeStringException(-1,"CDaliMutex::enter Cannot create /Locks branch");
                path.clear().appendf("Mutex[@name=\"%s\"]",name.get());
                if (!pconn->queryRoot()->hasProp(name))
                    pconn->queryRoot()->addPropTree("Mutex",createPTree("Mutex"))->setProp("@name",name);
                continue; // try again
            }
            unsigned remaining;
            if (tm.timedout(&remaining))
                break;
            // subscribed while still locked
            SubscriptionId subid = querySDS().subscribe(path.str(), *sub);
            conn.clear();
            unsigned ttw = first?1000*60:1000*60*5; // poll every 5m - may up later (once subscription spots auto-delete)
            if (!sem.wait((remaining>ttw)?ttw:remaining)) {
                if (!tm.timedout()) {
                    PROGLOG("Waiting for dali mutex %s",name.get());
                    if (first) {
                        if (notify) {
                            notify->startWait();
                            needstop = true;
                        }
                        first = false;
                    }
                    else if (needstop)
                        notify->cycleWait();
                }
            }
            querySDS().unsubscribe(subid);
        }
        if (needstop)
            notify->stopWait(false);
        count = 0;
        return false;
    }

    void leave()
    {
        CriticalBlock block(crit);
        if (!count)
            throw MakeStringException(-1,"CDaliMutex leave without corresponding enter");
        if (--count)
            return;
        oconn.clear();
    }

    void kill()
    {
        stopping = true;    // note must not be in crit sect
        sem.signal();
        CriticalBlock block(crit);
        count = 0;
        oconn.clear();
    }

};


IDaliMutex  *createDaliMutex(const char *name)
{
    return new CDaliMutex(name);
}

// ===============================================================================
// File redirection

/*

/Files
   /Redirection  @version
       /Maps

*/

class CDFSredirection: implements IDFSredirection, public CInterface
{
    MemoryAttr buf;
    unsigned version;
    unsigned lastloaded;
public:
    IMPLEMENT_IINTERFACE;

    struct sMapRec
    {
        const char *pat;
        const char *repl;
        bool iswild;

        bool match(const char *name,StringBuffer &out)
        {
            if (iswild)
                return WildMatchReplace(name,pat,repl,true,out);
            if (stricmp(name,pat)!=0)
                return false;
            out.append(repl);
            return true;
        }

    } *maps;
    unsigned nmaps;
    CriticalSection sect;
    unsigned linked;

    CDFSredirection()
    {
        linked = 0;
        maps = NULL;
        nmaps = 0;
        version = (unsigned)-1;
        lastloaded = 0;
    }

    ~CDFSredirection()
    {
        if (linked)
            IERRLOG("CDFSredirection: cDFSredirect leaked(%d)",linked);
        clear();
    }

    void clear()
    {
        buf.clear();
        delete [] maps;
        maps = NULL;
        nmaps = 0;
        lastloaded = 0;
    }

    class cDFSredirect: implements IDfsLogicalFileNameIterator, public CInterface
    {
        unsigned idx;
        unsigned got;
        StringAttr infn;
        CDfsLogicalFileName lfn;
        CDFSredirection &parent;
    public:
        IMPLEMENT_IINTERFACE;
        cDFSredirect(CDFSredirection &_parent,const char *_infn)
            : infn(_infn), parent(_parent)
        {
            // in crit sect
            idx = 0;
            got = (unsigned)-1;
        }
        ~cDFSredirect()
        {
            CriticalBlock block(parent.sect);
            parent.linked--;
        }

        bool first()
        {
            idx = (unsigned)-1;
            return next();
        }
        bool next()
        {
            StringBuffer out;
            for (;;) {
                idx++;
                if (idx>=parent.nmaps)
                    break;
                if (parent.maps[idx].match(infn.get(),out.clear())) {
                    if (out.length()==0) { // this is 'blocker'
                        idx=parent.nmaps;
                        break;
                    }
                    if (lfn.setValidate(out.str())) {
                        got = idx;
                        return true;
                    }
                }
            }
            got = (unsigned)-1;
            return false;
        }
        bool isValid()
        {
            return idx==got;
        }
        CDfsLogicalFileName & query()
        {
            return lfn;
        }
    };

    void load()
    {
        // called in critical section
        if (linked)
            return; // locked in
        if (lastloaded&&(lastloaded-msTick()<MIN_REDIRECTION_LOAD_INTERVAL))
            return; // loaded recently (can be cleared)
        Owned<IRemoteConnection> conn = querySDS().connect("Files/Redirection", myProcessSession(), RTM_LOCK_READ, SDS_LOCK_TIMEOUT);
        clear();
        lastloaded = msTick();
        if (!lastloaded)
            lastloaded++;
        if (conn) {
            IPropertyTree &root = *conn->queryRoot();
            unsigned v = (unsigned)root.getPropInt("@version",-1);
            if ((v!=(unsigned)-1)&&(v==version))
                return;
            version = v;
            clear();
            MemoryBuffer mb;
            if (root.getPropBin("Maps",mb))
                mb.read(nmaps);
            else
                nmaps = 0;
            if (nmaps) {
                maps = new sMapRec[nmaps];
                size32_t len = mb.length()-mb.getPos();
                mb.read(len,buf.allocate(len));
                const char *s = (const char *)buf.bufferBase();
                for (unsigned i=0;*s&&(i<nmaps);i++) {
                    maps[i].pat = s;
                    const char *r = s+strlen(s)+1;
                    maps[i].repl = r;
                    maps[i].iswild = (strchr(s,'*')||strchr(s,'?')||strchr(r,'$'));
                    s = r+strlen(r)+1;
                }
                // future stuff added here
            }
        }
        else
            clear();

    }

    void update(const char *targpat, const char *targrepl, unsigned idx)
    {
        // *doesn't* reload (but invalidates last load time)
        Owned<IRemoteConnection> conn = querySDS().connect("Files/Redirection", myProcessSession(), RTM_LOCK_WRITE|RTM_CREATE_QUERY, SDS_LOCK_TIMEOUT);
        if (!conn) {
            OERRLOG("Cannot update Files/Redirection");
            return;
        }
        IPropertyTree &root = *conn->queryRoot();
        MemoryBuffer mb;
        MemoryBuffer mbout;
        unsigned nm;
        const char * s;
        if (root.getPropBin("Maps",mb)) {
            mb.read(nm);
            s = (const char *)mb.readDirect(mb.length()-mb.getPos());
        }
        else {
            nm = 0;
            s = NULL;
        }
        unsigned i;
        unsigned no = 0;
        mbout.append(no);
        for (i=0;(i<nm)&&*s;i++) {
            if (i==idx) {
                no++;
                mbout.append(targpat);
                mbout.append(targrepl);
            }
            size32_t ls = strlen(s)+1;
            const char *r = s+ls;
            size32_t lr = strlen(r)+1;
            // see if matches (to delete old)
            if (stricmp(targpat,s)!=0) {
                no++;
                mbout.append(ls,s);
                mbout.append(lr,r);
            }
            s = r+lr;
        }
        if (i<idx) {
            no++;
            mbout.append(targpat);
            mbout.append(targrepl);
        }
        mbout.writeDirect(0,sizeof(no),&no);
        root.setPropBin("Maps",mbout.length(),mbout.toByteArray());
        root.setPropInt("@version",root.getPropInt("@version",-1)+1);
        CriticalBlock block(sect);
        lastloaded = 0;
    }

    IDfsLogicalFileNameIterator *getMatch(const char *infn)
    {
        CriticalBlock block(sect);
        load();
        linked++;
        return new cDFSredirect(*this,infn);
    }

    bool getEntry(unsigned idx,StringBuffer &pat,StringBuffer &repl)
    {
        CriticalBlock block(sect);
        load();
        if (idx>=nmaps)
            return false;
        pat.append(maps[idx].pat);
        repl.append(maps[idx].repl);
        return true;
    }

    unsigned numEntries()
    {
        CriticalBlock block(sect);
        load();
        return nmaps;
    }
};


IDFSredirection *createDFSredirection() // only called by dadfs.cpp
{
    return new CDFSredirection();
}

void safeChangeModeWrite(IRemoteConnection *conn,const char *name,bool &reload, unsigned timeoutms)
{
    unsigned start = msTick();
    unsigned count = 0;
    // if the lock was lost (changed to NONE), means someone else changed it
    // so the caller might want to refresh its cache (of sub-files, for ex.)
    reload = false;
#ifdef TEST_DEADLOCK_RELEASE
    // release the lock so that other threads can try and lock it
    conn->changeMode(RTM_NONE);
    reload = true; // always reload
#endif
    unsigned steptime = 1000*60*5;
    if ((timeoutms!=INFINITE)&&(steptime>timeoutms/2))
        steptime = timeoutms/2;
    for (;;) {
        try {
            if ((timeoutms!=INFINITE)&&(steptime>timeoutms))
                steptime = timeoutms;
            conn->changeMode(RTM_LOCK_WRITE,steptime,true);
            // lock was lost at least once, refresh connection
            if (reload) {
                conn->reload();
                PROGLOG("safeChangeModeWrite - re-obtained lock for %s",name);
            }
            break;
        }
        catch (ISDSException *e)
        {
            if (SDSExcpt_LockTimeout == e->errorCode())
            {
                unsigned tt = msTick()-start;
                if (count++>0) {// don't warn first time
                    DBGLOG("safeChangeModeWrite on %s waiting for %ds",name,tt/1000);
                    if (count==2)
                        PrintStackReport();
                }
                if (timeoutms!=INFINITE) {
                    timeoutms -= steptime;
                    if (timeoutms==0)
                        throw;
                }
                e->Release();
            }
            else
                throw;
        }
        // temporarily release the lock, we don't need to warn twice, do we?
        if (!reload) {
            DBGLOG("safeChangeModeWrite - temporarily releasing lock on %s to avoid deadlock",name);
            conn->changeMode(RTM_NONE);
            reload = true;
        }
        unsigned pause = 1000*(30+getRandom()%60);
        if (timeoutms!=INFINITE) 
            if (pause>timeoutms/2)
                pause = timeoutms/2;
        Sleep(pause);
        if (timeoutms!=INFINITE) 
            timeoutms -= pause;
    }
}

static bool transactionLoggingOn=false;
static cycle_t slowTransactionThreshold=0;
const bool &queryTransactionLogging() { return transactionLoggingOn; }
cycle_t querySlowTransactionThreshold() { return slowTransactionThreshold; }
bool traceAllTransactions()
{
    if (transactionLoggingOn)
        return false; // no change
    transactionLoggingOn = true;
    return true;
}

bool clearAllTransactions()
{
    if (!transactionLoggingOn)
        return false; // no change
    transactionLoggingOn = false;
    slowTransactionThreshold = 0;
    return true;
}

bool traceSlowTransactions(unsigned thresholdMs)
{
    if (thresholdMs)
    {
        cycle_t newSlowTransactionThreshold = nanosec_to_cycle(((unsigned __int64)thresholdMs)*1000000);
        bool changed = !transactionLoggingOn || (slowTransactionThreshold != newSlowTransactionThreshold);
        slowTransactionThreshold = newSlowTransactionThreshold;
        transactionLoggingOn = true;
        return changed;
    }
    else if (transactionLoggingOn) // was on, turning off
    {
        transactionLoggingOn = false;
        slowTransactionThreshold = 0;
        return true; // changed
    }
    else // was already off
        return false;
}

class CLockInfo : public CSimpleInterfaceOf<ILockInfo>
{
    StringAttr xpath;
    SafePointerArrayOf<CLockMetaData> ldInfo;
public:
    CLockInfo(MemoryBuffer &mb)
    {
        mb.read(xpath);
        unsigned count;
        mb.read(count);
        if (count)
        {
            ldInfo.ensureCapacity(count);
            for (unsigned c=0; c<count; c++)
                ldInfo.append(new CLockMetaData(mb));
        }
    }
    CLockInfo(const char *_xpath, const ConnectionInfoMap &map) : xpath(_xpath)
    {
        HashIterator iter(map);
        ForEach(iter)
        {
            IMapping &imap = iter.query();
            LockData *lD = map.mapToValue(&imap);
            ConnectionId connId = * ((ConnectionId *) imap.getKey());
            ldInfo.append(new CLockMetaData(*lD, connId));
        }
    }
// ILockInfo impl.
    virtual const char *queryXPath() const { return xpath; }
    virtual unsigned queryConnections() const { return ldInfo.ordinality(); }
    virtual CLockMetaData &queryLockData(unsigned lock) const
    {
        return *ldInfo.item(lock);
    }
    virtual void prune(const char *ipPattern)
    {
        StringBuffer ipStr;
        ForEachItemInRev(c, ldInfo)
        {
            CLockMetaData &lD = *ldInfo.item(c);
            SocketEndpoint ep(lD.queryEp());
            ep.getHostText(ipStr.clear());
            if (!WildMatch(ipStr, ipPattern))
                ldInfo.zap(&lD);
        }
    }
    virtual void serialize(MemoryBuffer &mb) const
    {
        mb.append(xpath);
        mb.append(ldInfo.ordinality());
        ForEachItemIn(c, ldInfo)
            ldInfo.item(c)->serialize(mb);
    }
    virtual StringBuffer &toString(StringBuffer &out, unsigned format, bool header, const char *altText) const
    {
        if (ldInfo.ordinality())
        {
            unsigned msNow = msTick();
            CDateTime time;
            time.setNow();
            time_t timeSimple = time.getSimple();

            if (header)
            {
                switch (format)
                {
                    case 0: // internal
                    {
                        out.append("Locks on path: ").append(altText ? altText : xpath.get()).newline();
                        out.append("Endpoint            |SessionId       |ConnectionId    |mode    |time(duration)]").newline();
                        break;
                    }
                    case 1: // daliadmin
                    {
                        out.appendf("Server IP           |Session         |Connection    |Mode    |Time                |Duration    |Lock").newline();
                        out.appendf("====================|================|==============|========|====================|============|======").newline();
                        break;
                    }
                    default:
                        throwUnexpected();
                }
            }

            CDateTime timeLocked;
            StringBuffer timeStr;
            unsigned c = 0;
            for (;;)
            {
                CLockMetaData &lD = *ldInfo.item(c);
                unsigned lockedFor = msNow-lD.timeLockObtained;
                time_t tt = timeSimple - (lockedFor/1000);
                timeLocked.set(tt);
                timeLocked.getString(timeStr.clear());

                switch (format)
                {
                    case 0: // internal
                        out.appendf("%-20s|%-16" I64F "x|%-16" I64F "x|%-8x|%s(%d ms)", lD.queryEp(), lD.sessId, lD.connectionId, lD.mode, timeStr.str(), lockedFor);
                        break;
                    case 1: // daliadmin
                        out.appendf("%-20s|%-16" I64F "x|%-16" I64F "x|%-8x|%-20s|%-12d|%s", lD.queryEp(), lD.sessId, lD.connectionId, lD.mode, timeStr.str(), lockedFor, altText ? altText : xpath.get());
                        break;
                    default:
                        throwUnexpected();
                }
                ++c;
                if (c>=ldInfo.ordinality())
                    break;
                out.newline();
            }

        }
        return out;
    }
};

ILockInfo *createLockInfo(const char *xpath, const ConnectionInfoMap &map)
{
    return new CLockInfo(xpath, map);
}

ILockInfo *deserializeLockInfo(MemoryBuffer &mb)
{
    return new CLockInfo(mb);
}

class CLockInfoCollection : public CSimpleInterfaceOf<ILockInfoCollection>
{
    CLockInfoArray locks;
public:
    CLockInfoCollection() { }
    CLockInfoCollection(MemoryBuffer &mb)
    {
        unsigned lockCount;
        mb.read(lockCount);
        for (unsigned l=0; l<lockCount; l++)
        {
            Owned<ILockInfo> lockInfo = deserializeLockInfo(mb);
            locks.append(* lockInfo.getClear());
        }
    }
// ILockInfoCollection impl.
    virtual unsigned queryLocks() const { return locks.ordinality(); }
    virtual ILockInfo &queryLock(unsigned lock) const { return locks.item(lock); }
    virtual void serialize(MemoryBuffer &mb) const
    {
        mb.append(locks.ordinality());
        ForEachItemIn(l, locks)
            locks.item(l).serialize(mb);
    }
    virtual StringBuffer &toString(StringBuffer &out) const
    {
        if (0 == locks.ordinality())
            out.append("No current locks").newline();
        else
        {
            ForEachItemIn(l, locks)
                locks.item(l).toString(out, 0, true).newline();
        }
        return out;
    }
    virtual void add(ILockInfo &lock) { locks.append(lock); }
};

ILockInfoCollection *createLockInfoCollection()
{
    return new CLockInfoCollection();
}

ILockInfoCollection *deserializeLockInfoCollection(MemoryBuffer &mb)
{
    return new CLockInfoCollection(mb);
}

static const char* remLeading(const char* s)
{
    if (*s == '/')
        s++;
    return s;
}

static unsigned daliConnectTimeoutMs = 5000;
extern da_decl IRemoteConnection* connectXPathOrFile(const char* path, bool safe, StringBuffer& xpath)
{
    CDfsLogicalFileName lfn;
    StringBuffer lfnPath;
    if ((strstr(path, "::") != nullptr) && !strchr(path, '/'))
    {
        lfn.set(path);
        lfn.makeFullnameQuery(lfnPath, DXB_File);
        path = lfnPath.str();
    }
    else if (strchr(path + ((*path == '/') ? 1 : 0),'/') == nullptr)
        safe = true;    // all root trees safe

    Owned<IRemoteConnection> conn = querySDS().connect(remLeading(path), myProcessSession(), safe ? 0 : RTM_LOCK_READ, daliConnectTimeoutMs);
    if (!conn && !lfnPath.isEmpty())
    {
        lfn.makeFullnameQuery(lfnPath.clear(), DXB_SuperFile);
        path = lfnPath.str();
        conn.setown(querySDS().connect(remLeading(path), myProcessSession(), safe? 0 : RTM_LOCK_READ, daliConnectTimeoutMs));
    }
    if (conn.get())
        xpath.append(path);
    return conn.getClear();
}

void addStripeDirectory(StringBuffer &out, const char *directory, const char *planePrefix, unsigned partNum, unsigned lfnHash, unsigned numStripes)
{
    if (numStripes <= 1)
        return;
    /* 'directory' is the prefix+logical file path, we need to know
    * the base plane prefix to manipulate it and insert the stripe directory.
    */
    if (!isEmptyString(planePrefix))
    {
        assertex(startsWith(directory, planePrefix));
        const char *tail = directory+strlen(planePrefix);
        if (isPathSepChar(*tail))
            tail++;
        out.append(planePrefix);
        assertex(lfnHash);
        unsigned stripeNum = calcStripeNumber(partNum, lfnHash, numStripes);
        addPathSepChar(out).append('d').append(stripeNum);
        if (*tail)
            addPathSepChar(out).append(tail);
    }
}

static CConfigUpdateHook directIOUpdateHook;
static CriticalSection dafileSrvNodeCS;
static Owned<INode> tlsDirectIONode, nonTlsDirectIONode;

unsigned getPreferredDaFsServerPort()
{
    return getPreferredDafsClientPort(true);
}

void remapGroupsToDafilesrv(IPropertyTree *file, bool foreign, bool secure)
{
    Owned<IPropertyTreeIterator> iter = file->getElements("Cluster");
    ForEach(*iter)
    {
        IPropertyTree &cluster = iter->query();
        const char *planeName = cluster.queryProp("@name");
        Owned<IStoragePlane> plane = getDataStoragePlane(planeName, true);
        if (isAbsolutePath(plane->queryPrefix())) // if url (i.e. not absolute prefix path) don't touch
        {
            if (isContainerized())
            {
                auto updateFunc = [&](const IPropertyTree *oldComponentConfiguration, const IPropertyTree *oldGlobalConfiguration)
                {
                    auto resolve = [&](bool secure) -> INode *
                    {
                        auto directioService = k8s::getDafileServiceFromConfig("directio", secure, false);
                        if (0 == directioService.second) // port. If 0, getDafileServiceFromConfig did not find a match
                            return nullptr;
                        VStringBuffer dafilesrvEpStr("%s:%u", directioService.first.c_str(), directioService.second);
                        const char *typeText = secure ? "secure" : "non-secure";
                        Owned<INode> directIONode = createINode(dafilesrvEpStr);
                        if (directIONode->endpoint().isNull())
                            throw makeStringExceptionV(0, "Unable to resolve %s directio dafilesrv hostname '%s'", typeText, directioService.first.c_str());
                        PROGLOG("%s directio = %s", typeText, dafilesrvEpStr.str());
                        return directIONode.getClear();
                    };
                    {
                        CriticalBlock b(dafileSrvNodeCS);
                        tlsDirectIONode.setown(resolve(true));
                        nonTlsDirectIONode.setown(resolve(false));
                    }
                };
                directIOUpdateHook.installOnce(updateFunc, true);
            }

            Owned<IGroup> group;
            if (cluster.hasProp("Group"))
                group.setown(createIGroup(cluster.queryProp("Group")));
            else
            {
                // JCSMORE only expected here if via foreign access (not entirely sure if this route is ever possible anymore)
                assertex(foreign);
                StringBuffer defaultDir;
                GroupType groupType;
                group.setown(queryNamedGroupStore().lookup(planeName, defaultDir, groupType));
            }

            std::vector<INode *> nodes;
            if (isContainerized())
            {
                Linked<INode> dafileSrvNodeCopy;
                {
                    // in case config hook above changes tlsDirectIONode/nonTlsDirectIONode
                    CriticalBlock b(dafileSrvNodeCS);
                    dafileSrvNodeCopy.set(secure ? tlsDirectIONode : nonTlsDirectIONode);
                }
                if (!dafileSrvNodeCopy)
                {
                    const char *typeText = secure ? "secure" : "non-secure";
                    throw makeStringExceptionV(0, "%s DFS service request made, but no %s directio service available", typeText, typeText);
                }
                for (unsigned n=0; n<group->ordinality(); n++)
                    nodes.push_back(dafileSrvNodeCopy);
            }
            else
            {
                // remap the group url's to explicitly contain the baremetal dafilesrv port configuration
                unsigned port = getPreferredDafsClientPort(true);
                for (unsigned n=0; n<group->ordinality(); n++)
                {
                    SocketEndpoint ep = group->queryNode(n).endpoint();
                    Owned<INode> newNode = createINodeIP(group->queryNode(n).endpoint(), port);
                    nodes.push_back(newNode);
                }
            }
            Owned<IGroup> newGroup = createIGroup((rank_t)group->ordinality(), &nodes[0]);
            StringBuffer groupText;
            newGroup->getText(groupText);
            cluster.setProp("Group", groupText);
        }
    }
}

#ifdef NULL_DALIUSER_STACKTRACE
static time_t lastNullUserLogEntry = (time_t)0;
static CriticalSection nullUserLogCS;
void logNullUser(IUserDescriptor * userDesc)
{
    StringBuffer userName;
    if (userDesc)
        userDesc->getUserName(userName);
    if (nullptr == userDesc || userName.isEmpty())
    {
        CriticalBlock block(nullUserLogCS);
        time_t timeNow = time(nullptr);
        if (difftime(timeNow, lastNullUserLogEntry) >= 60)
        {
            IERRLOG("UNEXPECTED USER (NULL)");
            PrintStackReport();
            lastNullUserLogEntry = timeNow;
        }
    }
}
#endif
