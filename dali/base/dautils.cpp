/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
############################################################################## */

#define da_decl __declspec(dllexport)

#include "platform.h"
#include "jlib.hpp"
#include "jstring.hpp"
#include "jfile.hpp"
#include "jmisc.hpp"
#include "jsort.hpp"
#include "jprop.hpp"
#include "jregexp.hpp"
#include "rmtfile.hpp"

#include "mpbase.hpp"
#include "dautils.hpp"
#include "dadfs.hpp"
#include "dasds.hpp"
#include "daclient.hpp"

#ifdef _DEBUG
//#define TEST_DEADLOCK_RELEASE
#endif

#define EXTERNAL_SCOPE      "file"
#define FOREIGN_SCOPE       "foreign"
#define SDS_DFS_ROOT        "Files" // followed by scope/name
#define SDS_RELATIONSHIPS_ROOT  "Files/Relationships"
#define SDS_CONNECT_TIMEOUT  (1000*60*60*2)     // better than infinite
#define MIN_REDIRECTION_LOAD_INTERVAL 1000

extern da_decl const char *queryDfsXmlBranchName(DfsXmlBranchKind kind)
{
    switch (kind) {
    case DXB_File:          return "File";
    case DXB_SuperFile:     return "SuperFile";
    case DXB_Collection:    return "Collection";
    case DXB_Scope:         return "Scope";
    }
    assertex(!"unknown DFS XML branch name");
    return "UNKNOWN";
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

inline bool validFNameChar(char c)
{
    static const char *invalids = "*\"/:<>?\\|";
    return (c>=32 && c<127 && !strchr(invalids, c));
}


class CMultiDLFN
{
    unsigned count;
    CDfsLogicalFileName *dlfns;

public:
    CMultiDLFN(const char *prefix,const StringArray &lfns)
    {
        unsigned c = lfns.ordinality();
        count = 0;
        dlfns = new CDfsLogicalFileName[c];
        StringBuffer lfn(prefix);
        size32_t l = lfn.length();
        for (unsigned i=0;i<c;i++) {
            const char * s = lfns.item(i);
            skipSp(s);
            if (*s=='~')
                s++;
            else {
                lfn.setLength(l);
                lfn.append(s);
                s = lfn.str();
            }
            dlfns[count++].set(s);
        }
    }

    CMultiDLFN(CMultiDLFN &other)
    {
        count = other.ordinality();
        dlfns = new CDfsLogicalFileName[count];
        ForEachItemIn(i,other) {
            dlfns[i].set(other.item(i));
        }
    }

    ~CMultiDLFN()
    {
        delete [] dlfns;
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
        mlfn.setLength(start-s);
        StringArray lfns;
        CslToStringArray(start+1, lfns);
        bool anywilds = false;
        ForEachItemIn(i1,lfns) {
            const char *suffix = lfns.item(i1);
            if (strchr(suffix,'*')||strchr(suffix,'?')) {
                anywilds = true;
                break;
            }
        }
        if (anywilds) {
            StringArray lfnout;
            StringBuffer tmp;
            ForEachItemIn(i2,lfns) {
                const char *suffix = lfns.item(i2);
                if (strchr(suffix,'*')||strchr(suffix,'?')) {
                    tmp.clear();
                    if (*suffix=='~')
                        tmp.append((strcmp(suffix+1,"*")==0)?"?*":suffix+1);
                    else
                        tmp.append(mlfn).append(suffix);
                    tmp.clip().toLowerCase();
                    Owned<IDFAttributesIterator> iter=queryDistributedFileDirectory().getDFAttributesIterator(tmp.str(),false,true);
                    mlfn.setLength(start-s);
                    ForEach(*iter) {
                        IPropertyTree &attr = iter->query();
                        if (!&attr)
                            continue;
                        const char *name = attr.queryProp("@name");
                        if (!name||!*name)
                            continue;
                        if (memicmp(name,mlfn.str(),mlfn.length())==0) // optimize
                            lfnout.append(name+mlfn.length());
                        else {
                            tmp.clear().append('~').append(name); // need leading ~ otherwise will get two prefixes
                            lfnout.append(tmp.str());
                        }
                    }
                }
                else
                    lfnout.append(suffix);
            }
            lfns.kill();
            ForEachItemIn(i3,lfnout) {
                lfns.append(lfnout.item(i3));
            }
        }
        if (lfns.ordinality()==0)
            return NULL;
        CMultiDLFN *ret =  new CMultiDLFN(mlfn.str(),lfns);
        if (ret->ordinality())
            return ret;
        delete ret;
        return NULL;
    }

    const CDfsLogicalFileName &item(unsigned idx)
    {
        assertex(idx<count);
        return dlfns[idx];
    }

    unsigned ordinality()
    {
        return count;
    }


};


CDfsLogicalFileName::CDfsLogicalFileName()
{
    allowospath = false;
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

void CDfsLogicalFileName::set(const CDfsLogicalFileName &other)
{
    lfn.set(other.lfn);
    tailpos = other.tailpos;
    localpos = other.localpos;
    cluster.set(other.cluster);
    external = other.external;
    delete multi;
    if (other.multi)
        multi = new CMultiDLFN(*other.multi);
    else
        multi = NULL;
}

bool CDfsLogicalFileName::isForeign() const
{
    if (localpos!=0)
        return true;
    if (multi) {
        ForEachItemIn(i1,*multi) {
            if (multi->item(i1).isForeign())        // if any are say all are
                return true;
        }
    }
    return false;
}

void CDfsLogicalFileName::set(const char *name)
{
    clear();
    StringBuffer str;
    if (!name)
        return;
    skipSp(name);
    try {
        multi = CMultiDLFN::create(name);
    }
    catch (IException *e) {
        StringBuffer err;
        e->errorMessage(err);
        WARNLOG("CDfsLogicalFileName::set %s",err.str());
        e->Release();
        multi = false;
    }
    if (multi) {
        StringBuffer full;
        full.append('{');
        ForEachItemIn(i1,*multi) {
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
    if (allowospath&&(isAbsolutePath(name)||(stdIoHandle(name)>=0)||(strstr(name,"::")==NULL))) {
        RemoteFilename rfn;
        rfn.setRemotePath(name);
        setExternal(rfn);
        return;
    }
    StringBuffer nametmp;
    const char *s = name;
    const char *ct = NULL;
    bool wilddetected = false;
    while (*s) {
        switch (*s) {
        case '@': ct = s; break;
        case ':': ct = NULL; break;
        case '?':
        case '*': wilddetected = true; break;
        case '~':
            if (s==name) { // leading ~ not allowed
                name++;
                skipSp(name);
                s = name;
                continue;
            }
            break;
        }
        s++;
    }
    bool isext = memicmp(name,EXTERNAL_SCOPE "::",sizeof(EXTERNAL_SCOPE "::")-1)==0;
    if (!isext&&wilddetected)
        throw MakeStringException(-1,"Wildcards not allowed in filename (%s)",name);
    if (!isext&&ct&&(ct-name>=1)) // trailing @
    {
        if ((ct[1]=='@')||(ct[1]=='^')) { // escape
            nametmp.append(ct-name,name).append(ct+1);
            name = nametmp.str();
        }
        else {
            nametmp.append(ct+1);
            nametmp.trim().toLowerCase();
            if (nametmp.length())
                cluster.set(nametmp);
            nametmp.clear().append(ct-name,name);   // treat trailing @ as no cluster
            name = nametmp.str();
        }
    }
    if (!*name)
        name = ".::_blank_";
    s=strstr(name,"::");
    if (s) {
        if (s==name)
            str.append('.');
        else {
            str.append(s-name,name).clip();
            if (isext) {    // normalize node
                const char *s1 = s+2;
                const char *ns1 = strstr(s1,"::");
                if (ns1) {                                          // TBD accept groupname here
                    skipSp(s1);
                    StringBuffer nodename;
                    nodename.append(ns1-s1,s1).clip();
                    SocketEndpoint ep(nodename.str());
                    if (!ep.isNull()) {
                        ep.getUrlStr(str.append("::"));
                        s = ns1;
                        external = true;
                        if (s[2]=='>') {
                            str.append("::");
                            tailpos = str.length();
                            str.append(s+2);
                            lfn.set(str);
                            return;
                        }

                    }
                }
            }
            else if (stricmp(str.str(),FOREIGN_SCOPE)==0) { // normalize node
                const char *s1 = s+2;
                const char *ns1 = strstr(s1,"::");
                if (ns1) {
                    skipSp(s1);
                    StringBuffer nodename;
                    nodename.append(ns1-s1,s1).clip();
                    SocketEndpoint ep(nodename.str());
                    if (!ep.isNull()) {
                        ep.getUrlStr(str.append("::"));
                        s = ns1;
                        localpos = str.length()+2;
                    }
                }
            }
        }
        loop {
            s+=2;
            const char *ns = strstr(s,"::");
            if (!ns)
                break;
            skipSp(s);
            str.append("::").append(ns-s,s).clip();
            s = ns;
        }
    }
    else {
        s = name;
        str.append(".");
    }
    str.append("::");
    tailpos = str.length();
    if (strstr(s,"::")!=NULL) {
        ERRLOG("Tail contains '::'!");
    }
    skipSp(s);
    str.append(s).clip().toLowerCase();
    lfn.set(str);
}


bool CDfsLogicalFileName::setValidate(const char *lfn,bool removeforeign)
{
    // NB will allows multi
    const char *s = lfn;
    if (!s)
        return false;
    skipSp(s);
    if (*s=='~') {  // allowed 1 leading ~
        s++;
        skipSp(s);
    }
    if (*s=='~')
        return false;
    if (allowospath&&(isAbsolutePath(s)||(stdIoHandle(s)>=0)||(strstr(lfn,"::")==NULL))) {
        set(lfn);
        return true;
    }

    const char *ns = skipScope(s,FOREIGN_SCOPE);
    if (ns) {
        while (*ns&&((*ns!=':')||(ns[1]!=':'))) // remove IP
            ns++;
        if (*ns) {
            s = ns+2;
            if (removeforeign) {
                lfn = s;
                skipSp(s);
            }
        }
    }
    const char *es = ns?NULL:skipScope(s,EXTERNAL_SCOPE);
    if (es) { 
        while (*es&&((*es!=':')||(es[1]!=':'))) // remove IP
            es++;
        if (*es) {
            if (es[2]=='>') { // query
                set(lfn);
                return true;
            }
            s = es+2;
        }
    }
    unsigned sc=0;
    int inmulti = 0;
    loop {
        while(*s!=':') {
            if (!*s) {
                if (sc==0)
                    return false;
                set(lfn);
                return true;
            }
            if (!validFNameChar(*s)) {
                if (!inmulti||((*s!='?')&&(*s!='*')))
                    return false;
            }
            if (*s=='{')
                inmulti++;
            else if (inmulti&&(*s=='}'))
                inmulti--;
            if (*s!=' ')
                sc++;
            s++;
        }
        if (sc==0)
            break;
        if (s[1]!=':')
            break;
        s += 2;
        sc = 0;
    }
    return false;
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
        WARNLOG("set with scopes called on multi-lfn %s",get());
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
    daliep.getUrlStr(str);
    str.append("::");
    str.append(get(true));
    set(str);
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
    IPropertyTree *ret = createPTree("SuperFile",false);
    unsigned numsub = 0;
    ForEachItemIn(i1,*multi) {
        const CDfsLogicalFileName &sub = multi->item(i1);
        IPropertyTree *st = createPTree(false);
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


void CDfsLogicalFileName::setExternal(const char *location,const char *path)
{
    if (!path||!*path)
        return;
    if (isPathSepChar(path[0])&&(path[0]==path[1])) {
        RemoteFilename rfn;
        rfn.setRemotePath(path);
        setExternal(rfn);  // overrides ip
        return;
    }
    StringBuffer str(EXTERNAL_SCOPE "::");
    str.append(location);
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
                str.append("::");
            else {
                if ((*path=='^')||isupper(*path))
                    str.append('^');
                str.append((char)tolower(*path));
            }
            path++;
        }
    }
    set(str.str());
}

void CDfsLogicalFileName::setExternal(const SocketEndpoint &dafsip,const char *path)
{
    StringBuffer str;
    dafsip.getUrlStr(str);
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
    rfsep.getUrlStr(str);
    setQuery(str.str(),query);
}


void CDfsLogicalFileName::setCluster(const char *cname)
{
    if (multi)
        WARNLOG("setCluster called on multi-lfn %s",get());
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
            WARNLOG("CDfsLogicalFileName::get with removeforeign set called on multi-lfn %s",get());
        ret += localpos;
    }
    return ret;
}

StringBuffer &CDfsLogicalFileName::get(StringBuffer &str,bool removeforeign) const
{
    return str.append(get(removeforeign));
}


const char *CDfsLogicalFileName::queryTail() const
{
    if (multi)
        WARNLOG("CDfsLogicalFileName::queryTail called on multi-lfn %s",get());
    const char *tail = get()+tailpos;
    if (strstr(tail,"::")!=NULL) {
        ERRLOG("Tail contains '::'!");
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
        WARNLOG("CDfsLogicalFileName::getScopes called on multi-lfn %s",get());
    // gets leading scopes without trailing ::
    const char *s =lfn.get();
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
        WARNLOG("CDfsLogicalFileName::getScopes called on multi-lfn %s",get());
    if (removeforeign)
        s += localpos;
    // num scopes = number of "::"s
    unsigned ret = 0;
    loop {
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
        WARNLOG("CDfsLogicalFileName::getScopes called on multi-lfn %s",get());
    if (removeforeign)
        s += localpos;
    // num scopes = number of "::"s
    loop {
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
        WARNLOG("CDfsLogicalFileName::makeFullnameQuery called on multi-lfn %s",get());
    if (absolute)
        query.append(SDS_DFS_ROOT "/");
    // returns full xpath for containing scope
    const char *s=get(true);    // skip foreign
    bool first=true;
    loop {
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

bool CDfsLogicalFileName::getEp(SocketEndpoint &ep) const
{
    SocketEndpoint nullep;
    ep = nullep;
    const char *ns;
    if (isExternal())
        ns = skipScope(lfn,EXTERNAL_SCOPE);
    else if (isForeign())
        ns = skipScope(lfn,FOREIGN_SCOPE);
    else
        ns = NULL;
    if (ns) {
        if (multi)
            WARNLOG("CDfsLogicalFileName::getEp called on multi-lfn %s",get());
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
        WARNLOG("CDfsLogicalFileName::makeFullnameQuery called on multi-lfn %s",get());
    const char *s = skipScope(lfn,EXTERNAL_SCOPE);
    if (s)
        s = strstr(s,"::");
    if (!s) {
        if (e)
            *e = MakeStringException(-1,"Invalid format for external file (%s)",get());
        return false;
    }
    if (s[2]=='>') {
        dir.append('/');
        tail.append(s+2);
        return true;
    }
    if (iswin&&(s[3]=='$'))
        s += 2;                 // no leading '\'
    const char *s1=s;
    const char *t1=NULL;
    loop {
        s1 = strstr(s1,"::");
        if (!s1)
            break;
        t1 = s1;
        s1 = s1+2;
    }
    if (!t1||!*t1) {
        if (e)
            *e = MakeStringException(-1,"No directory specified in external file name (%s)",get());
        return false;
    }
    bool start=true;
    size32_t odl = dir.length();
    while (s!=t1) {
        char c=*(s++);
        if (isPathSepChar(c)) {
            if (e)
                *e = MakeStringException(-1,"Path cannot contain separators, use '::' to seperate directories: (%s)",get());
            return false;
        }
        if ((c==':')&&(s!=t1)&&(*s==':')) {
            dir.append(iswin?'\\':'/');
            s++;
        }
        else if (c==':') {
            if (e)
                *e = MakeStringException(-1,"Path cannot contain single ':', use 'c$' to indicate 'c:' (%s)",get());
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
    if ((dir.length()!=odl)&&(!isPathSepChar(dir.charAt(dir.length()-1))))
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
    const char *dir = (rootdir&&*rootdir)?rootdir:queryBaseDirectory(false,os);
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
    loop {
        if (*fname==0)  // we didn't find tail
            return false;
        if (isPathSepChar(*fname))
            logicalName.append("::");
        else if (*fname=='.') {
            if (findPathSepChar(fname+1)==NULL) { // check for . mid name
                if (strchr(fname+1,'.')==NULL) { // check for multiple extension
                    fname++;
                    if (*fname=='_') {
                        loop {
                            fname++;
                            if (!*fname)
                                return false;
                            if (memicmp(fname,"_of_",4)==0) {
                                if (!fname[4])
                                    return false;
                                set(logicalName.str());
                                return true;
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
    IPropertyTree *pt = createPTree("Part",false);
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
        loop {
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
    loop {
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
    class cPartAttrIterator: public CInterface, implements IPropertyTreeIterator
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
    loop {
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
                ERRLOG("%s groups/numclusters mismatch",on);
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
                            ERRLOG("'%s' has duplicate cluster",on);
                        }
                        else
                            found[i] = true;
                        ok = true;
                        break;
                    }
                }
                if (!ok) {
                    const char * gs = pt->queryProp("@group");
                    ERRLOG("'%s' has missing cluster(%s) in groups(%s)",on,cname,gs?gs:"NULL");
                }
            }
            if (anyfound) {
                for (unsigned i=0;i<ng;i++)
                    if (!found[i])
                        WARNLOG("'%s' has missing group(%s) in clusters",on,groups.item(i));
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
    loop {
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
            parts[i] = createPTree("Part",false);
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
            ERRLOG("expandFileTree: Cluster %s not found in file",cluster);
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
                            node->endpoint().getIpText(ips.clear());
                            iter->query().setProp("@node",ips.str());
                        }
                    }
                }
            }
        }
        if (clusterinfo&&!file->hasProp("@replicated")) // legacy
            file->setPropBool("@replicated",clusterinfo->queryPartDiskMapping().defaultCopies>1);

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

inline void filteredAdd(IArrayOf<IPropertyTree> &results,const char *namefilterlo,const char *namefilterhi,IPropertyTree *item)
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
    item->Link();
    results.append(*item);
}


class cSort
{
    static CriticalSection sortsect;
    static cSort *sortthis;
    mutable CLargeMemoryAllocator buf;
    StringArray sortkeys;
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
    void dosort(IPropertyTreeIterator &iter,const char *sortorder, const char *namefilterlo,const char *namefilterhi, IArrayOf<IPropertyTree> &results)
    {
        StringBuffer sk;
        const char *s = sortorder;
        int mod = 0;
        loop {
            if (!*s||(*s==',')) {
                if (sk.length()) {
                    // could add '-' and '?' prefixes here (reverse/caseinsensitive)
                    sortkeys.append(sk.str());
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
            else
                sk.append(*s);
            s++;
        }
        ForEach(iter)
            filteredAdd(sortvalues,namefilterlo,namefilterhi,&iter.query());
        nv = sortvalues.ordinality();
        nk = sortkeys.ordinality();
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
        const char *key = sortkeys.item(k);
        const char *v;
        if ((key[0]=='@')&&(key[1]==0))
            v = sortvalues.item(idx).queryName();
        else
            v = sortvalues.item(idx).queryProp(key);
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
                int ret;
                if (mod&SORT_NUMERIC)
                    ret = (int)(_atoi64(v1)-_atoi64(v2));
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
                                     IArrayOf<IPropertyTree> &results)
{
    Owned<IRemoteConnection> conn = querySDS().connect(basexpath, myProcessSession(), 0, SDS_LOCK_TIMEOUT);
    if (!conn)
        return NULL;
    Owned<IPropertyTreeIterator> iter = conn->getElements(xpath);
    if (!iter)
        return NULL;
    if (namefilterlo&&!*namefilterlo)
        namefilterlo = NULL;
    if (namefilterhi&&!*namefilterhi)
        namefilterhi = NULL;
    StringBuffer nbuf;
    cSort sort;
    if (sortorder&&*sortorder)
        sort.dosort(*iter,sortorder,namefilterlo,namefilterhi,results);
    else
        ForEach(*iter)
            filteredAdd(results,namefilterlo,namefilterhi,&iter->query());
    return conn.getClear();
}


//==================================================================================

#define PAGE_CACHE_TIMEOUT (1000*60*10)

class CTimedCacheItem: extends CInterface
{
protected: friend class CTimedCache;
    StringAttr owner;
    unsigned due;
public:
    DALI_UID hint;
    IMPLEMENT_IINTERFACE;
    CTimedCacheItem(const char *_owner)
        : owner(_owner)
    {
        hint = queryCoven().getUniqueId();
        due = msTick()+PAGE_CACHE_TIMEOUT;
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
        unsigned res = (unsigned)-1;
        unsigned now = msTick();
        ForEachItemInRev(i,items) {
            CTimedCacheItem &item = items.item(i);
            unsigned t = item.due-now;
            if ((int)t<=0)
                items.remove(i);
            else if (t<res)
                res = t;
        }
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
        item->due = msTick()+PAGE_CACHE_TIMEOUT;
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
        thread.start();
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
    IMPLEMENT_IINTERFACE;
    CPECacheElem(const char *owner)
        : CTimedCacheItem(owner)
    {
    }
    ~CPECacheElem()
    {
    }
    Owned<IRemoteConnection> conn;
    IArrayOf<IPropertyTree> totalres;
};

IRemoteConnection *getElementsPaged( const char *basexpath,
                                     const char *xpath,
                                     const char *sortorder,
                                     unsigned startoffset,
                                     unsigned pagesize,
                                     ISortedElementsTreeFilter *postfilter, // filters before adding to page
                                     const char *owner,
                                     __int64 *hint,
                                     const char *namefilterlo,
                                     const char *namefilterhi,
                                     IArrayOf<IPropertyTree> &results)
{
    if (pagesize==0)
        return NULL;
    {
        CriticalBlock block(pagedElementsCacheSect);
        if (!pagedElementsCache) {
            pagedElementsCache = new CTimedCache;
            pagedElementsCache->start();
        }
    }
    Owned<CPECacheElem> elem;
    if (hint&&*hint)
        elem.setown(QUERYINTERFACE(pagedElementsCache->get(owner,*hint),CPECacheElem));
    if (!elem)
        elem.setown(new CPECacheElem(owner));
    if (!elem->conn)
        elem->conn.setown(getSortedElements(basexpath,xpath,sortorder,namefilterlo,namefilterhi,elem->totalres));
    if (!elem->conn)
        return NULL;
    unsigned n;
    if (postfilter) {
        n = 0;
        ForEachItemIn(i,elem->totalres) {
            IPropertyTree &item = elem->totalres.item(i);
            if (postfilter->isOK(item)) {
                if (n>=startoffset) {
                    item.Link();
                    results.append(item);
                    if (results.ordinality()>=pagesize)
                        break;
                }
                n++;
            }
        }
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
    IRemoteConnection *ret = elem->conn.getLink();
    if (hint) {
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
                      bool loadbranch)
{
    class cFileScanIterator: public CInterface, implements IClusterFileScanIterator
    {
        Owned<IPropertyTree> cur;
        unsigned fn;
        Linked<IRemoteConnection> conn;
        bool loadbranch;
        Linked<IGroup> lgrp;
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
            loop {
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
    Owned<IDFAttributesIterator> iter=queryDistributedFileDirectory().getDFAttributesIterator("*");
    StringBuffer chkname;
    StringBuffer gname;
    ForEach(*iter) {
        IPropertyTree &attr = iter->query();
        if (!&attr)
            continue;
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

void getLogicalFileSuperSubList(MemoryBuffer &mb)
{
    // for fileservices
    IsSuperFileMap supermap;
    IDFAttributesIterator *iter = queryDistributedFileDirectory().getDFAttributesIterator("*",true,true);
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
        if (!conn)
            throw MakeStringException(-1,"Could not connect to %s",lname.get());
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




class cDaliMutexSub: public CInterface, implements ISDSSubscription
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
//      PrintLog("Notification(%"I64F"x) of %s - flags = %d",(__int64) id, xpath, flags);
        sem.signal();
    }
};


class CDaliMutex: public CInterface, implements IDaliMutex
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
        while (!stopping) {
            // first lock semaphore to write
            StringBuffer path;
            path.appendf("/Locks/Mutex[@name=\"%s\"]",name.get());
            conn.setown(querySDS().connect(path.str(),mysession,RTM_LOCK_WRITE,SDS_CONNECT_TIMEOUT));
            if (conn) {
                SessionId oid = (SessionId)conn->queryRoot()->getPropInt64("Owner",0);
                if (!oid||querySessionManager().sessionStopped(oid,0)) {
                    StringBuffer opath(path);
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
                    pconn->queryRoot()->addPropTree("Mutex",createPTree("Mutex",false))->setProp("@name",name);
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

bool getSwapNodeInfo(IPropertyTree *options,StringAttr &grpname,Owned<IGroup> &grp,Owned<IRemoteConnection> &conn,Owned<IPropertyTree> &info, bool create)
{
    grpname.set(options->queryProp("@nodeGroup"));
    if (grpname.isEmpty())
        grpname.set(options->queryProp("@name"));
    if (grpname.isEmpty()) {
        ERRLOG("SWAPNODE - no group name specified in thor.xml");
        return false;
    }
    grp.setown(queryNamedGroupStore().lookup(grpname));
    if (!grp) {
        ERRLOG("SWAPNODE: group %s not found",grpname.get());
        return false;
    }
    conn.setown(querySDS().connect("/SwapNode", myProcessSession(), RTM_LOCK_WRITE|(create?RTM_CREATE_QUERY:0), 1000*60*5));
    if (!conn) {
        ERRLOG("SWAPNODE: could not connect to /SwapNode branch");
        return false;
    }
    StringBuffer xpath;
    xpath.appendf("Thor[@group=\"%s\"]",grpname.get());
    info.set(conn->queryRoot()->queryPropTree(xpath.str()));
    if (!info) {
        if (!create) {
            PROGLOG("SWAPNODE: no information for group %s",grpname.get());
            return false;
        }
        info.set(conn->queryRoot()->addPropTree("Thor",createPTree("Thor",false)));
        info->setProp("@group",grpname.get());
    }
    return true;
}



bool checkThorNodeSwap(IPropertyTree *options,const char *failedwuid, unsigned mininterval)
{
    bool ret = false;
    if (mininterval==(unsigned)-1) { // called by thor
        mininterval = 0;
        if (!options||!options->getPropBool("SwapNode/@autoSwapNode"))
            return false;
        if ((!failedwuid||!*failedwuid)&&!options->getPropBool("SwapNode/@checkAfterEveryJob"))
            return false;
    }

    try {
        Owned<IGroup> grp;
        Owned<IRemoteConnection> conn;
        Owned<IPropertyTree> info;
        StringAttr grpname;
        if (getSwapNodeInfo(options,grpname,grp,conn,info,true)) {
            PROGLOG("checkNodeSwap started");
            StringBuffer xpath;
            CDateTime dt;
            StringBuffer ts;
            // see if done less than mininterval ago
            if (mininterval) {
                dt.setNow();
                dt.adjustTime(-((int)mininterval));
                if (info->getProp("@timeChecked",ts)) {
                    CDateTime dtc;
                    dtc.setString(ts.str());
                    if (dtc.compare(dt,false)>0) {
                        PROGLOG("checkNodeSwap using cached validate from %s",ts.str());
                        xpath.clear().appendf("BadNode[@timeChecked=\"%s\"]",ts.str());
                        return info->hasProp(xpath.str());
                    }
                }
            }

            SocketEndpointArray epa;
            grp->getSocketEndpoints(epa);
            ForEachItemIn(i1,epa) {
                epa.item(i1).port = getDaliServixPort();
            }
            SocketEndpointArray failures;
            UnsignedArray failedcodes;
            StringArray failedmessages;
            unsigned start = msTick();
            validateNodes(epa,options->getPropBool("SwapNode/@swapNodeCheckC",true),options->getPropBool("SwapNode/@swapNodeCheckD",false),false,options->queryProp("SwapNode/@swapNodeCheckScript"),options->getPropInt("SwapNode/@swapNodeCheckScriptTimeout")*1000,failures,failedcodes,failedmessages);
            dt.setNow();
            dt.getString(ts.clear());
            ForEachItemIn(i,failures) {
                SocketEndpoint ep(failures.item(i));
                ep.port = 0;
                StringBuffer ips;
                ep.getIpText(ips);
                int r = (int)grp->rank(ep);
                if (r<0) {  // shouldn't occur
                    ERRLOG("SWAPNODE node %s not found in group %s",ips.str(),grpname.get());
                    continue;
                }
                PROGLOG("CheckSwapNode FAILED(%d) %s : %s",failedcodes.item(i),ips.str(),failedmessages.item(i));
                // SNMP TBD?

                ret = true;
                xpath.clear().appendf("BadNode[@netAddress=\"%s\"]",ips.str());
                IPropertyTree *bnt = info->queryPropTree(xpath.str());
                if (!bnt) {
                    bnt = info->addPropTree("BadNode",createPTree("BadNode",false));
                    bnt->setProp("@netAddress",ips.str());
                }
                bnt->setPropInt("@numTimes",bnt->getPropInt("@numTimes",0)+1);
                bnt->setProp("@timeChecked",ts.str());
                bnt->setProp("@time",ts.str());
                bnt->setPropInt("@code",failedcodes.item(i));
                bnt->setPropInt("@rank",r);
                bnt->setProp(NULL,failedmessages.item(i));
            }
            if (failedwuid&&*failedwuid) {
                xpath.clear().appendf("WorkUnit[@id=\"%s\"]",failedwuid);
                IPropertyTree *wut = info->queryPropTree(xpath.str());
                if (!wut) {
                    wut = info->addPropTree("WorkUnit",createPTree("WorkUnit",false));
                    wut->setProp("@id",failedwuid);
                }
                wut->setProp("@time",ts.str());
            }
            PROGLOG("checkNodeSwap: Time taken = %dms",msTick()-start);
            info->setProp("@timeChecked",ts.str());
        }
    }
    catch (IException *e) {
        EXCLOG(e,"checkNodeSwap");
    }
    return ret;
}


// ===============================================================================
// File redirection

/*

/Files
   /Redirection  @version
       /Maps

*/

class CDFSredirection: public CInterface, implements IDFSredirection
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
            ERRLOG("CDFSredirection: cDFSredirect leaked(%d)",linked);
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

    class cDFSredirect: public CInterface, implements IDfsLogicalFileNameIterator
    {
        unsigned idx;
        unsigned got;
        StringAttr infn;
        CDfsLogicalFileName lfn;
        CDFSredirection &parent;
    public:
        IMPLEMENT_IINTERFACE;
        cDFSredirect(CDFSredirection &_parent,const char *_infn)
            : parent(_parent), infn(_infn)
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
            loop {
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
            ERRLOG("Cannot update Files/Redirection");
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
        else
            nm = 0;
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

void safeChangeModeWrite(IRemoteConnection *conn,const char *name,bool &lockreleased, unsigned timeoutms)
{
    unsigned start = msTick();
    unsigned count = 0;
    lockreleased = false;
#ifdef TEST_DEADLOCK_RELEASE
    conn->changeMode(RTM_NONE); // thats the kludge
    lockreleased = true;
#endif
    unsigned steptime = 1000*60*5;
    if ((timeoutms!=INFINITE)&&(steptime>timeoutms/2))
        steptime = timeoutms/2;
    loop {
        try {
            if ((timeoutms!=INFINITE)&&(steptime>timeoutms))
                steptime = timeoutms;
            conn->changeMode(RTM_LOCK_WRITE,steptime,true);
            if (lockreleased) {
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
                    WARNLOG("safeChangeModeWrite on %s waiting for %ds",name,tt/1000);
                    if (count==2)
                        PrintStackReport();
                }
                e->Release();
                if (timeoutms!=INFINITE) {
                    timeoutms -= steptime;
                    if (timeoutms==0)
                        throw;
                }
            }
            else
                throw;
        }
        if (!lockreleased) {
            WARNLOG("safeChangeModeWrite - temporarily releasing lock on %s to avoid deadlock",name);
            conn->changeMode(RTM_NONE); // thats the kludge
            lockreleased = true;
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


class CLocalOrDistributedFile: public CInterface, implements ILocalOrDistributedFile
{
    Owned<IDistributedFile> dfile;
    CDfsLogicalFileName lfn;    // set if localpath but prob not useful
    StringAttr localpath;
public:
    IMPLEMENT_IINTERFACE;


    const char *queryLogicalName()
    {
        return localpath.isEmpty()?lfn.get():localpath.get();
    }
    IDistributedFile * queryDistributedFile() 
    { 
        return dfile.get(); 
    }

    bool init(const char *fname,IUserDescriptor *user,bool onlylocal,bool onlydfs, bool write)
    {
        if (!onlydfs)
            lfn.allowOsPath(true);
        if (!lfn.setValidate(fname))
            return false;
        if (!onlydfs) {
            bool gotlocal = true;
            if (isAbsolutePath(fname)||(stdIoHandle(fname)>=0)) 
                localpath.set(fname);
            else if (!strstr(fname,"::")) { // treat it as a relative file
                StringBuffer fn;
                localpath.set(makeAbsolutePath(fname,fn).str());
            }
            else if (!lfn.isExternal())
                gotlocal = false;
            if (gotlocal) {
                Owned<IFile> file = getPartFile(0,0);
                if (onlylocal)
                    dfile.setown(queryDistributedFileDirectory().lookup(lfn,user,write));
                if (file.get()&&(write||file->exists()))
                    return true;
            }
        }
        if (!onlylocal) {
            dfile.setown(queryDistributedFileDirectory().lookup(lfn,user,write));
            if (dfile.get()) {
                if (write&&lfn.isExternal()&&(dfile->numParts()==1))   // if it is writing to an external file then don't return distributed
                    dfile.clear();
                return true;
            }
        }
        return false;
    }

    IFileDescriptor *getFileDescriptor()
    {
        if (dfile.get())
            return dfile->getFileDescriptor();
        Owned<IFileDescriptor> fileDesc = createFileDescriptor();
        StringBuffer dir;
        if (localpath.isEmpty()) { // e.g. external file
            StringBuffer tail;
            IException *e=NULL;
            bool iswin=
#ifdef _WIN32
                true;
#else
                false;
#endif
            if (!lfn.getExternalPath(dir,tail,iswin,&e)) {
                if (e)
                    throw e;
                return NULL;
            }
        }
        else 
            splitDirTail(localpath,dir);
        fileDesc->setDefaultDir(dir.str());
        RemoteFilename rfn;
        getPartFilename(rfn,0,0);
        fileDesc->setPart(0,rfn);
        fileDesc->queryPartDiskMapping(0).defaultCopies = 1;
        return fileDesc.getClear();
    }

    bool getModificationTime(CDateTime &dt)
    {
        if (dfile.get())
            return dfile->getModificationTime(dt);
        Owned<IFile> file = getPartFile(0,0);
        if (file.get()) {
            CDateTime dt;
            return file->getTime(NULL,&dt,NULL);
        }
        return false;
    }

    virtual unsigned numParts()
    {
        if (dfile.get()) 
            return dfile->numParts();
        return 1;
    }


    unsigned numPartCopies(unsigned partnum)
    {
        if (dfile.get()) 
            return dfile->queryPart(partnum).numCopies();
        return 1;
    }
    
    IFile *getPartFile(unsigned partnum,unsigned copy)
    {
        RemoteFilename rfn;
        if ((partnum==0)&&(copy==0))
            return createIFile(getPartFilename(rfn,partnum,copy));
        return NULL;
    }
    
    RemoteFilename &getPartFilename(RemoteFilename &rfn, unsigned partnum,unsigned copy)
    {
        if (dfile.get()) 
            dfile->queryPart(partnum).getFilename(rfn,copy);
        else if (localpath.isEmpty())
            lfn.getExternalFilename(rfn);
        else
            rfn.setRemotePath(localpath);
        return rfn;
    }

    StringBuffer &getPartFilename(StringBuffer &path, unsigned partnum,unsigned copy)
    {
        RemoteFilename rfn;
        if (dfile.get()) 
            dfile->queryPart(partnum).getFilename(rfn,copy);
        else if (localpath.isEmpty())
            lfn.getExternalFilename(rfn);
        else 
            path.append(localpath);
        if (rfn.isLocal())
            rfn.getLocalPath(path);
        else
            rfn.getRemotePath(path);
        return path;
    }

    bool getPartCrc(unsigned partnum, unsigned &crc)
    {
        if (dfile.get())  
            return dfile->queryPart(partnum).getCrc(crc);
        Owned<IFile> file = getPartFile(0,0);
        if (file.get()) {
            crc = file->getCRC();
            return true;
        }
        return false;
    }

    offset_t getPartFileSize(unsigned partnum)
    {
        if (dfile.get()) 
            return dfile->queryPart(partnum).getFileSize(true,false);
        Owned<IFile> file = getPartFile(0,0);
        if (file.get())
            return file->size();
        return (offset_t)-1;
    }

    offset_t getFileSize()
    {
        if (dfile.get())
            dfile->getFileSize(true,false);
        offset_t ret = 0;
        unsigned np = numParts();
        for (unsigned i = 0;i<np;i++)
            ret += getPartFileSize(i);
        return ret;
    }


};

ILocalOrDistributedFile* createLocalDistributedFile(const char *fname,IUserDescriptor *user,bool onlylocal,bool onlydfs, bool iswrite)
{
    Owned<CLocalOrDistributedFile> ret = new CLocalOrDistributedFile();
    if (ret->init(fname,user,onlylocal,onlydfs,iswrite))
        return ret.getClear();
    return NULL;
}
