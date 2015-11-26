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

#pragma warning(disable:4786)
#include "platform.h"
#include "aci.ipp"
#include "ldapsecurity.ipp"

/****************************************************************
1 - SunOne aci syntax -
http://support.sas.com/rnd/itech/doc9/admin_ldap/security/ld_acc.html

(target="ldap:///dn")(targetattr="attrname")
   [(targetfilter="rfc2254-style filter")]
   ( version 3.0; acl "name"; (allow | deny)
   (read, write, search, compare, selfwrite, add, delete )
   (userdn | groupdn)="ldap:///dn";)

2 - OpenLdap aci syntax -
http://www.openldap.org/faq/data/cache/634.html

        < aci > ::= < acl syntax >
           < acl syntax > ::= <familyOID> + '#' + <scope > + '#'
                              + < rights >  + '#' + < dnType >
                              + '#' + < subjectDn >
           < subjectDn > ::= < printable string >
           < familyOid > ::= < oid >
           < scope > ::= "entry" | "subtree" | <level>
           < level > ::= numericstring
           < dnType > ::= "access-id" | "role" | "group" | "self"
           < rights > ::= [  ]   |   [ < right > + [ '$'
                          + <right> ] * ]
           < right > ::= <action > + ';' + <permissions>
                         + ';' +  <attrs>
           < action > ::= "grant" | "deny"
           < permissions > ::= [  ]  |   [ < permission >
                               + [ ',' + <permission> ] *  ]
           < attrs > ::= [ < attributeString>
                          + [ ',' + < attributeString > ] * ]
           < attributeString > ::= "[all]" | "[entry]"
                                   | <printableString >
           < permission > ::= "r" | "s" | "w" | "c"
    examples -
        OpenLDAPaci: 1#entry#grant;r,w,s,c;[all]#group#cn=enterprise admins,ou=groups,o=acme
        OpenLDAPaci: 2#entry#grant;r,w,s,c;[all]#group#cn=dallas admins,ou=groups,l=dallas,o=acme
        OpenLDAPaci: 3#entry#grant;r,w,s,c;userPassword,mail;r,s,c;[all]#access-id#uid=user1,ou=people,l=dallas,o=acme
        OpenLDAPaci: 4#entry#grant;r,s,c;[all]#group#cn=all acme,ou=groups,o=acme

3 - The ietf draft that none of them conform to (openldap is close) -
http://www.ietf.org/proceedings/02mar/I-D/draft-ietf-ldapext-acl-model-08.txt
*****************************************************************/

interface IAci : implements IInterface
{
    virtual StringBuffer& target() = 0;
    virtual StringArray& userdns() = 0;
    virtual StringArray& groupdns() = 0;
    virtual bool isDeny() = 0;
    virtual int permission() = 0;

    virtual StringBuffer& serialize(StringBuffer& acibuf) = 0;
    virtual void debugPrintout() = 0;
};

/****************************************************************
 *    Class CAci
 ****************************************************************/

class CAci : public CInterface, implements IAci
{
private:
    StringBuffer m_targetattr;
    StringBuffer m_target;
    StringBuffer m_version;
    StringBuffer m_name;
    StringArray  m_perms;
    bool         m_isDeny;
    StringArray  m_userdns;
    StringArray  m_groupdns;

    int          m_permission;

public:
    IMPLEMENT_IINTERFACE;
    
    CAci(bool _isDeny, int _perm, ACT_TYPE _act_type, const char* _dn)
    {
        m_isDeny = _isDeny;

        m_targetattr.append("*");
        m_version.append("3.0");
        Owned<IJlibDateTime> timeNow =  createDateTimeNow();
        SCMStringBuffer dateString;
        timeNow->getDateString(dateString);
        m_name.appendf("%s_%d", dateString.str(), getRandom());

        if(_act_type == GROUP_ACT)
            m_groupdns.append(_dn);
        else
            m_userdns.append(_dn);

        if((_perm & NewSecAccess_Full) == NewSecAccess_Full)
        {
            m_perms.append("all");
        }
        else
        {
            if((_perm & NewSecAccess_Write) == NewSecAccess_Write)
                m_perms.append("write");
            if((_perm & NewSecAccess_Read) == NewSecAccess_Read)
                m_perms.append("read");
            if((_perm & NewSecAccess_Access) == NewSecAccess_Access)
            {
                m_perms.append("compare");
                m_perms.append("search");
            }
        }
        m_permission = _perm;
    }

    CAci(const char* acistr)
    {
        //targetattr
        const char* curptr = strstr(acistr, "(targetattr");
        if(curptr != NULL)
        {
            while(*curptr != '"' && *curptr != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            while(*curptr != '"' && *curptr != '\0')
            {
                m_targetattr.append(*curptr);
                curptr++;
            }
        }

        // target
        curptr = strstr(acistr, "(target ");
        if(curptr == NULL)
            curptr = strstr(acistr, "target=");
        if(curptr != NULL)
        {
            while(*curptr != '"' && *curptr != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            if(strncmp(curptr, "ldap:///", 8) == 0)
                curptr += 8;

            while(*curptr != '"' && *curptr != '\0')
            {
                m_target.append(*curptr);
                curptr++;
            }
        }           
        
        curptr = strstr(acistr, "(version ");
        if(curptr != NULL)
        {
            //version
            while(*curptr != ' ' && *curptr != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            while(*curptr != ';' && *curptr != '\0')
            {
                m_version.append(*curptr);
                curptr++;
            }
            if(*curptr != '\0')
                curptr++;
            
            //name
            while(*curptr != ' ' && *curptr  != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            while(*curptr != '"' && *curptr != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            while(*curptr != '"' && *curptr != '\0')
            {
                m_name.append(*curptr++);
            }

            //permissions
            StringBuffer allow;
            while(*curptr == ' ' || *curptr == ';' || *curptr == '"')
                curptr++;

            while(*curptr != ' ' && *curptr != '\0')
                allow.append(*curptr++);
            m_isDeny = false;
            if(strcmp(allow.str(), "deny") == 0)
                m_isDeny = true;
            while(*curptr != '(' && *curptr != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            while(*curptr != '\0')
            {
                StringBuffer curperm;
                while(*curptr != ',' && *curptr != ')')
                {
                    curperm.append(*curptr);
                    curptr++;
                }
                m_perms.append(curperm.str());
                if(*curptr != '\0')
                    curptr++;
                if(*(curptr-1) == ')')
                    break;
            }

            // user/group dns
            while(*curptr != '(' && *curptr != '\0')
                curptr++;
            if(*curptr != '\0')
                curptr++;
            while(*curptr != '\0')
            {
                while(*curptr == ' ')
                    curptr++;
                StringBuffer dnname;
                while(*curptr != ' ' && *curptr != '=' && *curptr != '\0')
                {
                    dnname.append(*curptr);
                    curptr++;
                }
                while(*curptr != '"' && *curptr != '\0')
                    curptr++;
                if(*curptr != '\0')
                    curptr++;
                if(strncmp(curptr, "ldap:///", 8) == 0)
                    curptr += 8;
                StringBuffer dn;
                while(*curptr != '"' && *curptr != '\0')
                {
                    dn.append(*curptr);
                    curptr++;
                }
                if(*curptr != '\0')
                    curptr++;
                if(stricmp(dnname.str(), "userdn") == 0)
                    m_userdns.append(dn.str());
                else if(stricmp(dnname.str(), "groupdn") == 0)
                    m_groupdns.append(dn.str());
                while(*curptr == ' ')
                    curptr++;
                if(*curptr == ')')
                    break;
                while(*curptr != ' ' && *curptr != '\0')
                    curptr++;
            }
        }
        
        //calculate the secperm
        m_permission = 0;
        ForEachItemIn(y, m_perms)
        {
            const char* onepermstr = m_perms.item(y);
            if(onepermstr == NULL || *onepermstr == '\0')
                continue;
            if(stricmp(onepermstr, "all") == 0)
            {
                m_permission |= NewSecAccess_Full;
                break;
            }
            else if(stricmp(onepermstr, "read") == 0)
            {
                m_permission |= NewSecAccess_Read;
            }
            else if(stricmp(onepermstr, "write") == 0)
            {
                m_permission |= NewSecAccess_Write;
            }
            else if(stricmp(onepermstr, "compare") == 0 || stricmp(onepermstr, "search") == 0)
            {
                m_permission |= NewSecAccess_Access;
            }
        }
    }

    virtual StringBuffer& target()
    {
        return m_target;
    }

    virtual StringArray& userdns()
    {
        return m_userdns;
    }

    virtual StringArray& groupdns()
    {
        return m_groupdns;
    }

    virtual bool isDeny()
    {
        return m_isDeny;
    }

    virtual int permission()
    {
        return m_permission;
    }

    virtual StringBuffer& serialize(StringBuffer& acibuf)
    {
        acibuf.appendf("(targetattr = \"%s\") (version %s;acl \"%s\";", m_targetattr.str(), m_version.str(), m_name.str());
        acibuf.append(m_isDeny?"deny (":"allow (");
        unsigned i;
        for(i = 0; i < m_perms.length(); i++)
        {
            if(i > 0)
                acibuf.append(",");
            acibuf.append(m_perms.item(i));
        }
        acibuf.append(")(");
        int ind = 0;
        for(i = 0; i < m_userdns.length(); i++)
        {
            if(ind > 0)
                acibuf.append(" or ");
            acibuf.appendf("userdn = \"ldap:///%s\"", m_userdns.item(i));
            ind++;
        }
        for(i = 0; i < m_groupdns.length(); i++)
        {
            if(ind > 0)
                acibuf.append(" or ");
            acibuf.appendf("groupdn = \"ldap:///%s\"", m_groupdns.item(i));
            ind++;
        }

        acibuf.append(");)");

        return acibuf;
    }

    virtual void debugPrintout()
    {
        printf("name: %s\nversion: %s\ntarget: <%s>\ntargetattr: <%s>\n", m_name.str(), m_version.str(), m_target.str(), m_targetattr.str());
        if(m_isDeny)
            printf("deny:\n");
        else
            printf("allow:\n");
        unsigned y;
        for(y = 0; y < m_perms.length(); y++)
        {
            printf("\t%s\n", m_perms.item(y));
        }
        printf("\nuserdns:\n");
        for(y = 0; y < m_userdns.length(); y++)
        {
            printf("\t%s\n", m_userdns.item(y));
        }
        printf("groupdns:\n");
        for(y = 0; y < m_groupdns.length(); y++)
        {
            printf("\t%s\n", m_groupdns.item(y));
        }
    }
};

/****************************************************************
 *    Class COpenLdapAci
 ****************************************************************/

class COpenLdapAci : public CInterface, implements IAci
{
private:
    StringBuffer m_name;
    StringBuffer m_target;
    StringBuffer m_perms;
    StringArray  m_userdns;
    StringArray  m_groupdns;
    bool         m_isDeny;
    int          m_permission;

public:
    IMPLEMENT_IINTERFACE;
    
    COpenLdapAci(bool _isDeny, int _perm, ACT_TYPE _act_type, const char* _dn)
    {
        m_isDeny = _isDeny;

        Owned<IJlibDateTime> timeNow =  createDateTimeNow();
        SCMStringBuffer dateString;
        timeNow->getDateString(dateString);
        m_name.appendf("%s_%d", dateString.str(), getRandom());

        if(_act_type == GROUP_ACT)
            m_groupdns.append(_dn);
        else
            m_userdns.append(_dn);

        if((_perm & NewSecAccess_Full) == NewSecAccess_Full)
        {
            m_perms.append("r,w,d");
        }
        else
        {
            if((_perm & NewSecAccess_Write) == NewSecAccess_Write)
            {
                if(m_perms.length() != 0)
                    m_perms.append(",");
                m_perms.append("w");
            }
            if((_perm & NewSecAccess_Read) == NewSecAccess_Read)
            {
                if(m_perms.length() != 0)
                    m_perms.append(",");
                m_perms.append("r");
            }
        }
        m_permission = _perm;
    }

    COpenLdapAci(const char* acistr)
    {
        if(!acistr || !*acistr)
            return;
        const char* bptr = acistr;
        const char* eptr = acistr;

        // <OID>
        while(*bptr == ' ')
            bptr++;
        eptr = strchr(bptr, '#');
        if(eptr == NULL)
            throw MakeStringException(-1, "Invalid OpenLDAPaci format");
        m_name.append(eptr - bptr, bptr);

        //skip <scope>
        eptr++;
        eptr = strchr(eptr, '#');
        if(!eptr)
            throw MakeStringException(-1, "Invalid OpenLDAPaci format");

        //process <rights>
        bptr = eptr + 1;
        while(*bptr == ' ')
            bptr++;
        eptr = strchr(bptr, '#');
        if(!eptr)
            throw MakeStringException(-1, "Invalid OpenLDAPaci format");
        if(strncmp(bptr, "deny", 4) == 0)
        {
            m_isDeny = true;
            bptr += 4;
        }
        else if(strncmp(bptr, "grant", 5) == 0)
        {
            m_isDeny = false;
            bptr += 5;
        }
        else
            throw MakeStringException(-1, "Invalid OpenLDAPaci format");
        while(bptr <= eptr && (*bptr == ' ' || *bptr == ';'))
            bptr++;
        while(bptr <= eptr && *bptr != ';')
        {
            m_perms.append(*bptr);
            bptr++;
        }

        // <dntype>
        bool isGroup = false;
        eptr = strchr(bptr, '#');
        if(eptr == NULL)
            throw MakeStringException(-1, "Invalid OpenLDAPaci format");
        bptr = eptr + 1;
        while(*bptr == ' ')
            bptr++;
        if(strncmp(bptr, "group", 5) == 0 || strncmp(bptr, "role", 4) == 0)
            isGroup = true;

        // <subjectDN>      
        eptr = strchr(bptr, '#');
        if(eptr == NULL)
            throw MakeStringException(-1, "Invalid OpenLDAPaci format");
        bptr = eptr + 1;
        while(*bptr == ' ')
            bptr++;
        if(isGroup)
        {
            m_groupdns.append(bptr);
        }
        else
        {
            m_userdns.append(bptr);
        }

        //calculate the secperm
        m_permission = 0;
        if(strchr(m_perms.str(), 'r') != NULL && strchr(m_perms.str(), 'w') != NULL && strchr(m_perms.str(), 'd') != NULL)
        {
            m_permission |= NewSecAccess_Full;
        }
        else
        {
            if(strchr(m_perms.str(), 'r') != NULL)
            {
                m_permission |= NewSecAccess_Read;
            }

            if(strchr(m_perms.str(), 'w') != 0)
            {
                m_permission |= NewSecAccess_Write;
            }
        }
    }

    virtual StringBuffer& target()
    {
        return m_target;        
    }

    virtual StringArray& userdns()
    {
        return m_userdns;
    }

    virtual StringArray& groupdns()
    {
        return m_groupdns;
    }

    virtual bool isDeny()
    {
        return m_isDeny;
    }

    virtual int permission()
    {
        return m_permission;
    }

    virtual StringBuffer& serialize(StringBuffer& acibuf)
    {
        acibuf.appendf("%s#entry#%s;%s;[all]#", m_name.str(), m_isDeny?"deny":"grant", m_perms.str());
        if(m_groupdns.length() > 0)
            acibuf.appendf("group#%s", m_groupdns.item(0));
        else if(m_userdns.length() > 0)
            acibuf.appendf("access-id#%s", m_userdns.item(0));
        
        return acibuf;
    }

    virtual void debugPrintout()
    {
        printf("name: %s\n", m_name.str());
        if(m_isDeny)
            printf("deny:\n");
        else
            printf("allow:\n");
        printf("\t%s\n", m_perms.str());
        printf("\nuserdns:\n");
        unsigned y;
        for(y = 0; y < m_userdns.length(); y++)
        {
            printf("\t%s\n", m_userdns.item(y));
        }
        printf("groupdns:\n");
        for(y = 0; y < m_groupdns.length(); y++)
        {
            printf("\t%s\n", m_groupdns.item(y));
        }
    }
};

int NewSec2Sec(int newsec)
{
    int sec = 0;
    if(newsec == -1)
        return -1;
    if(newsec == NewSecAccess_Full)
        return SecAccess_Full;

    if((newsec & NewSecAccess_Write) == NewSecAccess_Write)
        sec |= SecAccess_Write;
    if((newsec & NewSecAccess_Read) == NewSecAccess_Read)
        sec |= SecAccess_Read;
    if((newsec & NewSecAccess_Access) == NewSecAccess_Access)
        sec |= SecAccess_Access;

    return sec;
}

/****************************************************************
 *    Class CAciList
 ****************************************************************/

class CAciList : public CInterface, implements IInterface
{
private:
    IArrayOf<IAci> m_acilist;
    LdapServerType m_servertype;

public:
    IMPLEMENT_IINTERFACE;

    CAciList(LdapServerType servertype, MemoryBuffer& acibuf)
    {
        m_servertype = servertype;

        const char* acistr = acibuf.toByteArray();
        int len = acibuf.length();
        int ci = 0;
        while(ci < len)
        {
            int bp = ci;
            while(acistr[ci] != '\0' && ci < len)
                ci++;
            
            if(servertype == IPLANET)
                m_acilist.append(*(new CAci(acistr+bp)));
            else
                m_acilist.append(*(new COpenLdapAci(acistr+bp)));

            while(acistr[ci] == '\0' && ci < len)
                ci++;
        }
    }

    bool getPermissions(ISecUser& user, ISecResource& resource, ILdapClient* ldapclient, const char* dn)
    {
        const char* res_name = resource.getName();
        ILdapConfig* ldapconfig = ldapclient->getLdapConfig();

        if(stricmp(ldapconfig->getSysUser(), user.getName()) == 0)
        {
            resource.setAccessFlags(SecAccess_Full);
            return true;
        }

        int perm = 0;
        if(m_acilist.length() == 0)
        {
            perm = -1;
        }
        else
        {
            int allow = 0;
            int deny = 0;
            ForEachItemIn(x, m_acilist)
            {
                IAci& aci = m_acilist.item(x);
                bool applicable = false;
                
                // check if target matches the dn of the resource
                if(aci.target().length() > 0 && (dn == NULL || stricmp(aci.target().str(), dn) != 0))
                    continue;

                //check if this aci is applicable to this user
                StringBuffer userdn;
                userdn.append("uid=").append(user.getName()).append(",").append(ldapconfig->getUserBasedn());
                ForEachItemIn(z, aci.userdns())
                {
                    const char* onedn = aci.userdns().item(z);
                    if(onedn != NULL && (stricmp(onedn, "anyone") == 0 || stricmp(onedn, userdn.str()) == 0))
                    {
                        applicable = true;
                        break;
                    }
                }
                
                if(!applicable)
                {
                    // See if the user belongs to one of the groups
                    ForEachItemIn(g, aci.groupdns())
                    {
                        const char* onegdn = aci.groupdns().item(g);
                        if(onegdn == NULL || *onegdn == '\0')
                            continue;
                        if(ldapclient->userInGroup(userdn.str(), onegdn))
                        {
                            applicable =true;
                            break;
                        }
                    }
                }

                if(applicable)
                {
                    if(aci.isDeny())
                        deny |= aci.permission();
                    else
                        allow |= aci.permission();
                }

            }

            perm = allow & (~deny);

            perm = NewSec2Sec(perm);
        }
        
        resource.setAccessFlags(perm);
        return true;
    }

    void getPermissionsArray(IArrayOf<CPermission>& permissions)
    {
        ForEachItemIn(x, m_acilist)
        {
            IAci& aci = m_acilist.item(x);
            int allows = 0;
            int denies = 0;
            if(aci.isDeny())
                denies = aci.permission();
            else
                allows = aci.permission();
            unsigned i;
            for(i = 0; i < aci.groupdns().length(); i++)
            {
                const char* dn = aci.groupdns().item(i);
                if(dn == NULL || dn[0] == '\0')
                    continue;

                StringBuffer name;
                LdapUtils::getName(dn, name);

                bool found = false;
                ForEachItemIn(x, permissions)
                {
                    CPermission& p = permissions.item(x);
                    if(stricmp(p.getAccount_name(), name.str()) == 0 && p.getAccount_type() == GROUP_ACT)
                    {
                        p.setAllows(p.getAllows() | allows);
                        p.setDenies(p.getDenies() | denies);
                        found = true;
                    }
                }
                if(!found)
                {
                    CPermission* p = new CPermission(name.str(), GROUP_ACT, allows, denies);
                    permissions.append(*p);
                }
            }
            for(i = 0; i < aci.userdns().length(); i++)
            {
                const char* dn = aci.userdns().item(i);
                if(dn == NULL || dn[0] == '\0')
                    continue;

                StringBuffer name;
                LdapUtils::getName(dn, name);

                bool found = false;
                ForEachItemIn(x, permissions)
                {
                    CPermission& p = permissions.item(x);
                    if(stricmp(p.getAccount_name(), name.str()) == 0 && p.getAccount_type() == USER_ACT)
                    {
                        p.setAllows(p.getAllows() | allows);
                        p.setDenies(p.getDenies() | denies);
                        found = true;
                    }
                }
                if(!found)
                {
                    CPermission* p = new CPermission(name.str(), USER_ACT, allows, denies);
                    permissions.append(*p);
                }
            }
        }
    }

    void changePermission(CPermissionAction& action)
    {
        if(action.m_account_type == GROUP_ACT && strncmp(action.m_account_name.str(), "cn=Directory Administrators", strlen("cn=Directory Administrators")) == 0)
        {
            if(stricmp(action.m_action.str(), "delete") != 0 && action.m_denies != 0)
                throw MakeStringException(-1, "Please don't set deny permissions for Directory Administrators");
        }
        if(action.m_account_type == USER_ACT && stricmp(action.m_account_name.str(), "anyone") == 0)
        {
            if(stricmp(action.m_action.str(), "delete") != 0 && action.m_denies != 0)
                throw MakeStringException(-1, "Please don't set deny permissions for anyone");
        }

        // if not add (means it's either update or delete), delete original aci.
        if(stricmp(action.m_action.str(), "add") != 0)
        {
            ForEachItemInRev(i, m_acilist)
            {
                IAci* aci = &m_acilist.item(i);
                if(action.m_account_type == GROUP_ACT)
                {
                    ForEachItemInRev(j, aci->groupdns())
                    {
                        const char* grpdn = aci->groupdns().item(j);
                        if(stricmp(grpdn, action.m_account_name.str()) == 0)
                        {
                            aci->groupdns().remove(j);
                            break;
                        }
                    }
                }
                else 
                {
                    ForEachItemInRev(j, aci->userdns())
                    {
                        const char* usrdn = aci->userdns().item(j);
                        if(stricmp(usrdn, action.m_account_name.str()) == 0)
                        {
                            aci->userdns().remove(j);
                            break;
                        }
                    }
                }

                if(aci->groupdns().length() + aci->userdns().length() == 0)
                    m_acilist.remove(i);
            }
        }

        // If not delete (means it's update or add), add the new aci
        if(stricmp(action.m_action.str(), "delete") != 0)
        {
            if(action.m_allows != 0)
            {
                if(m_servertype == IPLANET)
                    m_acilist.append(*(new CAci(false, action.m_allows, action.m_account_type, action.m_account_name)));
                else
                    m_acilist.append(*(new COpenLdapAci(false, action.m_allows, action.m_account_type, action.m_account_name)));
            }
            if(action.m_denies != 0)
            {
                if(m_servertype == IPLANET)
                    m_acilist.append(*(new CAci(true, action.m_denies, action.m_account_type, action.m_account_name)));
                else
                    m_acilist.append(*(new COpenLdapAci(true, action.m_denies, action.m_account_type, action.m_account_name)));
            }
        }
    }

    void debugPrintout()
    {
        ForEachItemIn(x, m_acilist)
        {
            printf("---------\n");
            IAci& aci = m_acilist.item(x);
            aci.debugPrintout();
        }
    }

    MemoryBuffer& serialize(MemoryBuffer& aclbuf)
    {
        ForEachItemIn(x, m_acilist)
        {
            IAci* aci = &m_acilist.item(x);
            if(aci == NULL)
                continue;
            StringBuffer acibuf;
            aci->serialize(acibuf);
            aclbuf.append(acibuf.length(), acibuf.str());
            aclbuf.append('\0');
        }

        return aclbuf;
    }
};

/****************************************************************
 *    Class AciProcessor 
 ****************************************************************/

AciProcessor::AciProcessor(IPropertyTree* cfg)
{
    if(cfg == NULL)
        throw MakeStringException(-1, "AciProcessor() - config is NULL");
    m_cfg.set(cfg);

    m_sidcache.setown(createPTree());
    m_cfg->getProp(".//@ldapAddress", m_server);
}

bool AciProcessor::getPermissions(ISecUser& user, IArrayOf<CSecurityDescriptor>& sdlist, IArrayOf<ISecResource>& resources)
{
    for(unsigned i = 0; i < sdlist.length(); i++)
    {
        CSecurityDescriptor& sd = sdlist.item(i);
        ISecResource& res = resources.item(i);
        CAciList acilist(m_servertype, sd.getDescriptor());
        acilist.getPermissions(user, res, m_ldap_client, sd.getDn());
    }

    return true;
}


CSecurityDescriptor* AciProcessor::createDefaultSD(ISecUser * const user, ISecResource* resource, SecPermissionType ptype)
{
    return createDefaultSD(user, resource->getName(), ptype);   
}

StringBuffer& AciProcessor::sec2aci(int secperm, StringBuffer& aciperm)
{
    throw MakeStringException(-1, "You should call the implementation of the child class");
}

CSecurityDescriptor* AciProcessor::createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype)
{
    throw MakeStringException(-1, "You should call the implementation of the child class");
}

CSecurityDescriptor* AciProcessor::createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd)
{
    throw MakeStringException(-1, "You should call the implementation of the child class");
}

bool AciProcessor::retrieveUserInfo(ISecUser& user)
{
    CLdapSecUser* ldapuser = (CLdapSecUser*)&user;
    const char* username = user.getName();
    if(username == NULL || strlen(username) == 0)
    {
        DBGLOG("AciProcessor::retrieveUserInfo : username is empty");
        return false;
    }

    const char* fullname = user.getFullName();
    if((fullname == NULL || *fullname == '\0') && m_ldap_client != NULL)
    {
        m_ldap_client->getUserInfo(user);
    }
    return true;
}

void AciProcessor::getCachedSid(const char* name, MemoryBuffer& sid)
{
    StringBuffer buf;
    if(toXpath(name, buf))
    {
        synchronized block(m_mutex);
        m_sidcache->getPropBin(buf.str(), sid);
    }
}

void AciProcessor::cacheSid(const char* name, int len, const void* sidbuf)
{
    StringBuffer buf;
    if(toXpath(name, buf))
    {
        synchronized block(m_mutex);
        m_sidcache->addPropBin(buf.str(), len, sidbuf);
    }
}

void AciProcessor::lookupSid(const char* act_name, MemoryBuffer& act_sid, ACT_TYPE acttype)
{
    throw MakeStringException(-1, "You shouldn't need function lookupSid");

    act_sid.clear();

    MemoryBuffer mb;
    getCachedSid(act_name, mb);
    if(mb.length() > 0)
    {
        act_sid.append(mb.length(), mb.toByteArray());
    }
    else
    {
        if(m_ldap_client != NULL)
        {
            m_ldap_client->lookupSid(act_name, act_sid, acttype);
            cacheSid(act_name, act_sid.length(), (const void*)act_sid.toByteArray());
        }
    }
}

int AciProcessor::sdSegments(CSecurityDescriptor* sd)
{
    if(sd == NULL || sd->getDescriptor().length() == 0)
        return 0;

    const char* sdptr = sd->getDescriptor().toByteArray();
    unsigned curind = 0;
    int segs = 0;
    bool endofone = true;
    while(curind < sd->getDescriptor().length())
    {
        if(*sdptr == '\0')
        {
            while(*sdptr == '\0' && curind < sd->getDescriptor().length())
            {
                sdptr++;
                curind++;
            }
            endofone = true;
        }
        else
        {
            if(endofone)
            {
                segs++;
                endofone = false;
            }
            sdptr++;
            curind++;
        }
    }

    return segs;
}

bool AciProcessor::getPermissionsArray(CSecurityDescriptor *sd, IArrayOf<CPermission>& permissions)
{
    CAciList acilist(m_servertype, sd->getDescriptor());
    acilist.getPermissionsArray(permissions);
    return true;
}

CSecurityDescriptor* AciProcessor::changePermission(CSecurityDescriptor* initialsd, CPermissionAction& action)
{
    CAciList acl(m_servertype, initialsd->getDescriptor());
    acl.changePermission(action);
    MemoryBuffer resultbuf;
    acl.serialize(resultbuf);
    CSecurityDescriptor* csd = new CSecurityDescriptor(action.m_rname.str());
    csd->setDescriptor(resultbuf.length(), (void*)resultbuf.toByteArray());
    return csd;

}

/****************************************************************
 *    Class CIPlanetAciProcessor 
 ****************************************************************/

StringBuffer& CIPlanetAciProcessor::sec2aci(int secperm, StringBuffer& aciperm)
{
    if(secperm == -1 || secperm == SecAccess_None)
        return aciperm;

    if(secperm >= SecAccess_Full)
    {
        aciperm.append("all");
        return aciperm;
    }

    // compare or search maps to SecAccess_Access, read to SecAccess_Read and write to SecAccess_Write
    if((secperm & SecAccess_Access) == SecAccess_Access)
        aciperm.append("compare search");
    if((secperm & SecAccess_Read) == SecAccess_Read)
        aciperm.append(" read");
    if((secperm & SecAccess_Write) == SecAccess_Write)
        aciperm.append(" write");
    
    return aciperm;
}

CSecurityDescriptor* CIPlanetAciProcessor::createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype)
{
    CSecurityDescriptor* csd = new CSecurityDescriptor(name);
    
    if(ptype != PT_ADMINISTRATORS_ONLY)
    {
        if(DEFAULT_AUTHENTICATED_USERS_PERMISSION != SecAccess_None)
        {
            StringBuffer defaultperm;
            sec2aci(DEFAULT_AUTHENTICATED_USERS_PERMISSION, defaultperm);
            StringBuffer default_sd;
            default_sd.append("(targetattr = \"*\") (version 3.0;acl \"default_aci\";allow (").append(defaultperm.str()).append(")(userdn = \"ldap:///anyone\");)");
            csd->setDescriptor(default_sd.length(), (void*)default_sd.str());
        }
    }
    else
    {
        ILdapConfig* ldapconfig = m_ldap_client->getLdapConfig();
        StringBuffer default_sd;
        default_sd.append("(targetattr = \"*\") (version 3.0;acl \"default_aci\";allow (all)(groupdn = \"ldap:///cn=Directory Administrators,").append(ldapconfig->getBasedn()).append("\");)");
        csd->setDescriptor(default_sd.length(), (void*)default_sd.str());
    }

    return csd;
}

CSecurityDescriptor* CIPlanetAciProcessor::createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd)
{
    if(resource == NULL)
        return NULL;

    CSecurityDescriptor* csd = new CSecurityDescriptor(resource->getName());
    if(initial_sd.length() > 0)
        csd->setDescriptor(initial_sd.length(), (void *)initial_sd.toByteArray());

    const char* userbasedn = NULL;
    if(m_ldap_client != NULL)
    {
        ILdapConfig* ldapconfig = m_ldap_client->getLdapConfig();
        if(ldapconfig != NULL)
            userbasedn = ldapconfig->getUserBasedn();
    }
    if(userbasedn == NULL)
        return csd;

    if(user && DEFAULT_OWNER_PERMISSION != SecAccess_None)
    {
        StringBuffer defaultperm;
        sec2aci(DEFAULT_OWNER_PERMISSION, defaultperm);
        StringBuffer default_sd;
        default_sd.append("(targetattr = \"*\") (version 3.0;acl \"default_aci\";allow (").append(defaultperm.str()).append(")");
        default_sd.append("(userdn = \"ldap:///");
        default_sd.append("uid=").append(user->getName());
        default_sd.append(",").append(userbasedn).append("\");)");
        csd->appendDescriptor(default_sd.length(), (void*)default_sd.str());
    }
    return csd;
}

/****************************************************************
 *    Class COpenLdapAciProcessor 
 ****************************************************************/

StringBuffer& COpenLdapAciProcessor::sec2aci(int secperm, StringBuffer& aciperm)
{
    if(secperm == -1 || secperm == SecAccess_None)
        return aciperm;

    if(secperm >= SecAccess_Full)
    {
        aciperm.append("r,w,d");
        return aciperm;
    }

    // compare or search maps to SecAccess_Access, read to SecAccess_Read and write to SecAccess_Write
    if((secperm & SecAccess_Read) == SecAccess_Read)
    {
        if(aciperm.length() > 0)
            aciperm.append(",");
        aciperm.append("r");
    }

    if((secperm & SecAccess_Write) == SecAccess_Write)
    {
        if(aciperm.length() > 0)
            aciperm.append(",");
        aciperm.append("w");
    }
    
    return aciperm;
}

CSecurityDescriptor* COpenLdapAciProcessor::createDefaultSD(ISecUser * const user, const char* name, SecPermissionType ptype)
{
    CSecurityDescriptor* csd = new CSecurityDescriptor(name);
    
    if(ptype != PT_ADMINISTRATORS_ONLY)
    {
        if(DEFAULT_AUTHENTICATED_USERS_PERMISSION != SecAccess_None)
        {
            StringBuffer defaultperm;
            sec2aci(DEFAULT_AUTHENTICATED_USERS_PERMISSION, defaultperm);
            StringBuffer default_sd;
            default_sd.appendf("default_aci#entry#grant;%s;[all]#access-id#anyone", defaultperm.str());
            csd->setDescriptor(default_sd.length(), (void*)default_sd.str());
        }
    }
    else
    {
        ILdapConfig* ldapconfig = m_ldap_client->getLdapConfig();
        StringBuffer default_sd;
        default_sd.appendf("default_aci#entry#grant;r,w,d;[all]#group#cn=Directory Administrators,%s", ldapconfig->getBasedn());
        csd->setDescriptor(default_sd.length(), (void*)default_sd.str());
    }

    return csd;
}

CSecurityDescriptor* COpenLdapAciProcessor::createDefaultSD(ISecUser * const user, ISecResource* resource, MemoryBuffer& initial_sd)
{
    if(resource == NULL)
        return NULL;

    CSecurityDescriptor* csd = new CSecurityDescriptor(resource->getName());
    if(initial_sd.length() > 0)
        csd->setDescriptor(initial_sd.length(), (void *)initial_sd.toByteArray());

    const char* userbasedn = NULL;
    if(m_ldap_client != NULL)
    {
        ILdapConfig* ldapconfig = m_ldap_client->getLdapConfig();
        if(ldapconfig != NULL)
            userbasedn = ldapconfig->getUserBasedn();
    }
    if(userbasedn == NULL)
        return csd;

    if(user && DEFAULT_OWNER_PERMISSION != SecAccess_None)
    {
        StringBuffer defaultperm;
        sec2aci(DEFAULT_OWNER_PERMISSION, defaultperm);
        StringBuffer default_sd;
        default_sd.appendf("default_aci#entry#grant;%s;[all]#access-id#uid=%s,%s", defaultperm.str(), user->getName(), userbasedn);
        csd->appendDescriptor(default_sd.length(), (void*)default_sd.str());
    }
    return csd;
}
