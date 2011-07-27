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

// Dali Authorization Capability Library
#ifndef DACAP_LINKED_IN
#define dacaplib_decl __declspec(dllexport)
#endif
#include "platform.h"

#include "jlib.hpp"
#include "jmisc.hpp"
#include "jarray.hpp"
#include "jfile.hpp"

static CBuildVersion _bv("$HeadURL: https://svn.br.seisint.com/ecl/trunk/dali/security/dacaplib.cpp $ $Id: dacaplib.cpp 62376 2011-02-04 21:59:58Z sort $");

#include "dacaplib.hpp"
#include "dasess.hpp"

#define SC_MAC_SZ    6 
#define SC_CPUSN_SZ  8 
#define SC_SYSTEM_SZ 8 // first bytes of given system string
#define SC_USAGE_SZ  2


#ifdef _WIN32
extern "C" {
    typedef DWORD (CALLBACK* LPSENDARP)(ULONG,ULONG,PULONG,PULONG);
}
#endif

unsigned netAddr(const IpAddress &ip)
{
    return (*(unsigned*)&ip);
}

bool getMAC(const IpAddress &ip,StringBuffer &out)
{
#ifdef _WIN32
    HMODULE Iphlpapi = LoadLibrary("Iphlpapi");
    if (!Iphlpapi)
        return false;
    LPSENDARP sendarp = (LPSENDARP) GetProcAddress(Iphlpapi,"SendARP");
    if (!sendarp) {
        FreeLibrary(Iphlpapi);
        return false;
    }
    unsigned  ipaddr = netAddr(ip);
    DWORD mac[2];
    memset (mac, 0xff, sizeof (mac));
    DWORD maclen = 6;
    HRESULT hr = sendarp (ipaddr, 0, mac, &maclen);
    if (hr!=0) {

        // error TBD
        return false;
    }
    byte *p = (byte *)&mac[0];
    for (unsigned i=0;i<6;i++) {
        if (i)
            out.append('-');
        out.appendf("%02X",(unsigned)*p);
        p++;
    }
    FreeLibrary(Iphlpapi);
    return true;
#else
    return false; // linux TBD
#endif
}

bool checkKey(const char *keystr)
{
    // 48 hex digits without separators
    for (unsigned i = 0; i<FIXED_KEY_SIZE*2; i++) {
        if (!isxdigit(*(keystr++)))
            return false;
    }
    return true;
}


bool decodeMAC(const char *macstr,byte *mac)
{
    // 12 hex digits with or without separators
    memset(mac,0,SC_MAC_SZ);
    for (unsigned i = 0; i<SC_MAC_SZ; i++) {
        if (*macstr=='-')
            macstr++;
        char c1 = *(macstr++);
        if (!isxdigit(c1))
            return false;
        char c2 = *(macstr++);
        if (!isxdigit(c1))
            return false;
        *(mac++) = (byte)((hex2num(c1)<<4)+hex2num(c2));
    }
    return true;
}

bool decodeCPUSN(const char *cpusnstr,byte *cpusn)
{
    // 16 hex digits with or without separators
    memset(cpusn,0,SC_CPUSN_SZ);
    if (!cpusnstr)
        return false;

    for (unsigned i = 0; i<SC_CPUSN_SZ; i++) {
        if (*cpusnstr=='-')
            cpusnstr++;
        int v1 = hex2num(*(cpusnstr++));
        if (v1==-1)
            return false;
        int v2 = hex2num(*(cpusnstr++));
        if (v2==-1)
            return false;
        *(cpusn++) = (byte)(v1*16+v2);
    }
    return true;
}

enum XMLentrykind { EK_setlimit=1,EK_addnode,EK_removenode};

#define CRCMASK (0x5128F1EE)

class CDaliCapabilityCreator: public CInterface, implements IDaliCapabilityCreator
{
    StringAttr sysid;
    StringAttr clientpassword;
    StringAttr serverpassword;
    Owned<IPropertyTree> root;
    unsigned crc;

    void addcap(const char *tag,CSystemCapability &cap)
    { // cap is plain
        cap.setSystem(sysid.get());
        IPropertyTree *tree = root->addPropTree(tag,createPTree(tag));
        crc = crc32(tag,strlen(tag),crc);
        if (clientpassword)
            cap.secure((const byte *)clientpassword.get(), clientpassword.length());
        if (serverpassword)
            cap.secure((const byte *)serverpassword.get(), serverpassword.length());
        StringBuffer keystr;
        const byte *key=(const byte *)cap.queryKey();
        crc = crc32((const char *)key,FIXED_KEY_SIZE,crc);
        for (unsigned i=0;i<FIXED_KEY_SIZE;i++)
            keystr.appendf("%02X",(unsigned)key[i]);
        tree->setProp("@cap",keystr.str());
    }
public:
    IMPLEMENT_IINTERFACE;
    CDaliCapabilityCreator()
    {
        reset();
    }
    void setSystemID(const char *systemid)
    {
        sysid.set(systemid);
    }
    void setClientPassword(const char *password)
    {
        clientpassword.set(password);
    }
    void setServerPassword(const char *password)
    {
        serverpassword.set(password);
    }
    void addCapability(DaliClientRole role,const char *macstr,const char *cpusnstr=NULL)
    {
        CSystemCapability cap;
        byte cpusn[SC_CPUSN_SZ];
        if (cpusnstr&&decodeCPUSN(cpusnstr,cpusn))
            cap.setCpuSN(cpusn);
        byte mac[SC_MAC_SZ];
        if (macstr&&decodeMAC(macstr,mac))
            cap.setMAC(mac); 
        cap.setRole(role);
        addcap(role==DCR_DaliServer?"srv":"add",cap);
    }
    void removeCapability(DaliClientRole role,const char *macstr,const char *cpusnstr=NULL)
    {
        CSystemCapability cap;
        byte cpusn[SC_CPUSN_SZ];
        if (cpusnstr&&decodeCPUSN(cpusnstr,cpusn))
            cap.setCpuSN(cpusn);
        byte mac[SC_MAC_SZ];
        if (macstr&&decodeMAC(macstr,mac))
            cap.setMAC(mac); 
        cap.setRole(role);
        addcap("del",cap);
    }
    virtual void setLimit(DaliClientRole role,unsigned limit)
    {
        CSystemCapability cap;
        cap.setRole(role);
        cap.setLimit(limit);
        addcap("set",cap);
    }

    void save(StringBuffer &text)
    {
        root->setProp("@system",sysid.get());
        crc = crc32((const char *)sysid.get(),sysid.length(),crc);  
        root->setPropInt64("@update",crc^CRCMASK);
        toXML(root, text);      
    }
    void reset()
    {
        root.setown(createPTree("DACAP"));
        crc = 0;
    }
};


IDaliCapabilityCreator *createDaliCapabilityCreator()
{
    return new CDaliCapabilityCreator;
}


static const char DACONF_FILENAME[] = "daliconf.xml";
static const char DASYSTEM_XPATH[]  = "@system";
static const char DACAP_XPATH[]     = "@cap";
static const char UPDATE_XPATH[]    = "Update";


unsigned importDaliCapabilityXML_basic(const char *filename)
{

    Owned<IPropertyTree> root;
    try {
        root.setown(createPTreeFromXMLFile(filename));
    }
    catch (IException *e) {
        // no specific error (file missing)
        e->Release();
        return 70;
    }
    // first validate
    unsigned crc = 0;
    Owned<IPropertyTreeIterator> it = root->getElements("*");
    ForEach(*it) {
        const char *tag = it->query().queryName();
        crc = crc32(tag,strlen(tag),crc);
        CSystemCapability sc(it->query().queryProp("@cap"));
        crc = crc32((const char *)sc.queryKey(),FIXED_KEY_SIZE,crc);
    }
    const char *sysid=root->queryProp("@system");
    crc = crc32(sysid,strlen(sysid),crc);   
    if (root->getPropInt64("@update")!=(crc^CRCMASK))
        return 71;
    Owned<IPropertyTree>conf;
    try {
        conf.setown(createPTreeFromXMLFile(DACONF_FILENAME));
    }
    catch (IException *e) {
        // no specific error (file missing)
        e->Release();
        return 72;
    }
    const char *sysidold = conf->queryProp(DASYSTEM_XPATH); 
    bool modified = false;
    if (!sysidold||!*sysidold) {
        conf->setProp(DASYSTEM_XPATH,sysid);
        modified = true;
    }
    else if (strcmp(sysidold,sysid)!=0)
        return 73;
    if (stricmp(conf->queryName(),"DALI")!=0)
        return 76;
    IPropertyTree *update = NULL;
    ForEach(*it) {
        const char *tag = it->query().queryName();
        if (strcmp(tag,"set")==0) {
            if (!update) {
                update = conf->queryPropTree(UPDATE_XPATH);
                if (!update)
                    update = conf->addPropTree(UPDATE_XPATH,createPTree(UPDATE_XPATH));
            }
            IPropertyTree *tree = update->addPropTree(tag,createPTree(tag));
            tree->setProp("@cap",it->query().queryProp("@cap"));
            modified = true;
        }
        else if (strcmp(tag,"srv")==0) {
            const char *oldcap = conf->queryProp(DACAP_XPATH);
            const char *cap = it->query().queryProp("@cap");
            if (!oldcap||(strcmp(cap,oldcap)!=0)) { // wrong so update
                conf->setProp(DACAP_XPATH,cap);
                modified = true;
            }
        }
        else
            return 74; // basic scheme doesn't support add/delete
    }
    if (modified) { // which it probably will be!
        try {
            Owned<IFile> ifile = createIFile(DACONF_FILENAME);
            Owned<IFileIO> ifileio = ifile->open(IFOcreate);
            StringBuffer res;
            toXML(conf,res,false);
            ifileio->write(0,res.length(),res.str());
        }
        catch (IException *e) {
            // no specific error (file connot be written)
            e->Release();
            return 75;
        }
    }
    return 0;
}




#if 1

#include "mpbase.hpp"
#include "dadfs.hpp"

#define CAP_CLIENT_PASSWORD_XYZ400A  "fdkkfgfgf"
#define CAP_SERVER_PASSWORD_XYZ400A  "trerrtrtf"

void test()
{
    Owned<IDaliCapabilityCreator> cc = createDaliCapabilityCreator();
    cc->setSystemID("XYZ400A");
    cc->setClientPassword(CAP_CLIENT_PASSWORD_XYZ400A);
    cc->setServerPassword(CAP_SERVER_PASSWORD_XYZ400A);
    Owned<IGroup> cluster = queryNamedGroupStore().lookup("XYZ_THOR400");
    ForEachNodeInGroup(i,*cluster) {
        INode &node = cluster->queryNode(i);
        StringBuffer mac;
        getMAC(node.endpoint().ip,mac);
        cc->addCapability(DCR_ThorSlave,mac.str());
    }
    StringBuffer results;
    cc->save(results);
}

#endif
