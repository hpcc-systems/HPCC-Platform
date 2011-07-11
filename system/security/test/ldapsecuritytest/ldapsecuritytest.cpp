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

#include <stdlib.h>
#include <stdio.h>

#include "seclib.hpp"
#include "ldapsecurity.hpp"
#include "jliball.hpp"
#include "thirdparty.h"
#include <set>
#include <string.h>

#ifdef _WIN32
#include <conio.h>
#else
#include <unistd.h>
#endif

Mutex m_mutex;

void usage()
{
    printf("usage: ldapsecuritytest -ac|-au|-ar|-cp -c configfile [-u <username>] [-p <password>] [-r <resource>] [-t <resourcetype>] [-np <newpassword>] [-fn <firstname>] [-ln lastname]\n");
    printf("-ca: check access\n");
    printf("-au: add user\n");
    printf("-ar: add resource\n");
    printf("-cp: change password\n");
    printf("-t <resourcetype>: resource type can be one of the following values - \n");
    printf("                   resource, module, filescope, workunit\n");
    printf("                   default is resource\n");
}

void inputpassword(const char* prompt, StringBuffer& passwd)
{
    passwd.clear();
#ifdef _WIN32
    printf("%s", prompt);
    char input=0;
    short num_entries=0;
    while (0x0d != (input = (char)getch()))
    {
        if (input == '\b')
        {
            printf("\b \b");
            if (num_entries)
            { 
                num_entries--; 
            }
            continue;
        }
        passwd.append(input);
        num_entries++;
        printf("*");
    }
    printf("\n");
#else
    const char* pass = getpass(prompt);
    passwd.append(pass);
#endif
}

void getpassword(const char* prompt, StringBuffer& passwd, bool verify = true)
{
    passwd.clear();

    StringBuffer passwd1, passwd2;
    int tries = 0;
    while(1)
    {
        if(tries++ >= 3)
        {
            exit(-1);
        }

        inputpassword(prompt, passwd1);
        if(!verify)
            break;

        inputpassword("Verifying password, retype: ", passwd2);
        if(passwd1.length() < 4)
        {
            printf("password too short, should be 4 characters or longer\n");
        }
        else if(strcmp(passwd1.str(), passwd2.str()) != 0)
        {
            printf("passwords don't match.\n");
        }
        else
            break;
    }

    passwd.append(passwd1.str());

}

class CPermissionCheckThread : public Thread
{
    ISecManager* m_secmgr;
    StringAttr m_user, m_passwd, m_resource;
    SecResourceType m_rtype;
    int m_rounds;
public:
    IMPLEMENT_IINTERFACE;

    CPermissionCheckThread(ISecManager* secmgr, const char* user, const char* passwd, const char* r, SecResourceType rtype, int rounds)
    {
        m_secmgr = secmgr;
        m_user.set(user);
        m_passwd.set(passwd);
        m_resource.set(r);
        m_rtype = rtype;
        m_rounds = rounds;
    }

    virtual int run()
    {
        int access = 0;
        int total = 0, mint = -1, maxt = 0;
        for(int i = 0; i < m_rounds; i++)
        {
            time_t start, stop;
            time(&start);
            {
                //synchronized block(m_mutex);
                Owned<ISecUser> usr = m_secmgr->createUser(m_user.get());
                usr->credentials().setPassword(m_passwd.get());
                //access = m_secmgr->authorizeFileScope(*usr, m_resource.get());
                access = m_secmgr->authorizeEx(m_rtype, *usr, m_resource.get());
            }
            time(&stop);
            int span = stop - start;
            total += span;
            if(mint == -1 || mint > span)
                mint = span;
            if(maxt < span)
                maxt = span;
            if((i+1)%100 == 0)
                DBGLOG("Finished %d times\n", i+1);
        }
        DBGLOG("Permission: %d, min: %d, max: %d, average:%f", access, mint, maxt, total*1.0/m_rounds);
        return 0;
    }
};

int main(int argc, char* argv[])
{
    if(argc < 2)
    {
        usage();
        return -1;
    }

    InitModuleObjects();

    const char *action = NULL, *configfile = NULL, *username = NULL, *passwd = NULL, 
        *resource = NULL, *resourcetype = NULL, *newpasswd = NULL, *firstname = NULL, *lastname=NULL;

    bool stress = false;
    int numthrds = 0;
    int numrounds = 0;
    int numfiles = 0;

    int i = 1;
    while(i<argc)
    {
        if (stricmp(argv[i], "-ac")==0 || stricmp(argv[i], "-au") == 0 || stricmp(argv[i], "-ar") == 0|| stricmp(argv[i], "-cp") == 0)
        {
            action = argv[i++];
        }
        else if (stricmp(argv[i], "-c")==0)
        {
            i++;
            configfile = argv[i++];
        }
        else if (stricmp(argv[i],"-u")==0)
        {
            i++;
            username = argv[i++];
        }
        else if (stricmp(argv[i], "-p")==0)
        {
            i++;
            passwd = argv[i++];
        }
        else if (stricmp(argv[i], "-r")==0)
        {
            i++;
            resource = argv[i++];
        }
        else if (stricmp(argv[i], "-t") == 0)
        {
            i++;
            resourcetype = argv[i++];
        }
        else if (stricmp(argv[i], "-np") == 0)
        {
            i++;
            newpasswd = argv[i++];
        }
        else if (stricmp(argv[i], "-fn") == 0)
        {
            i++;
            firstname = argv[i++];
        }
        else if (stricmp(argv[i], "-ln") == 0)
        {
            i++;
            lastname = argv[i++];
        }
        else if (stricmp(argv[i], "-stress") == 0)
        {
            stress = true;
            i++;
            numthrds = atoi(argv[i++]);
            numrounds = atoi(argv[i++]);
        }
        else if (stricmp(argv[i], "-open") == 0)
        {
            i++;
            numfiles = atoi(argv[i++]);
        }
        else
        {
            printf("Error: command format error\n");
            usage();
            return -1;
        }
    }

    if(configfile == NULL || *configfile == '\0')
    {
        printf("You have to specify the config file");
        return -1;
    }
    
    try
    {
        Owned<IPropertyTree> cfg = createPTreeFromXMLFile(configfile, false);
        Owned<IPropertyTree> seccfg = cfg->getPropTree(".//ldapSecurity");
        if(seccfg == NULL)
        {
            printf("ldapSecurity not found\n");
            return -1;
        }
#ifdef _NO_LDAP
        printf("System was built with _NO_LDAP\n");
        return -1;
#else
        Owned<ISecManager> secmgr = newLdapSecManager("test", *LINK(seccfg));
        if(secmgr == NULL)
        {
            printf("security manager can't be created\n");
            return -1;
        }

        if(action == NULL || stricmp(action, "-ac") == 0)
        {
            if(username == NULL || *username == '\0')
            {
                printf("missing username\n");
                return -1;
            }
            if(resource == NULL || *resource == '\0')
            {
                printf("missing resource\n");
                return -1;
            }

            SecResourceType rtype = RT_DEFAULT;
            if((resourcetype != NULL) && (stricmp(resourcetype, "filescope") == 0))
                rtype = RT_FILE_SCOPE;
            else if((resourcetype != NULL) && (stricmp(resourcetype, "workunit") == 0))
                rtype = RT_WORKUNIT_SCOPE;

            StringBuffer passbuf;
            if(passwd == NULL || *passwd == '\0')
            {
                getpassword("Enter password: ", passbuf, false);
                passwd = passbuf.str();
            }

            if(!stress)
            {
                Owned<ISecUser> usr = secmgr->createUser(username);
                usr->credentials().setPassword(passwd);
                int access = secmgr->authorizeEx(rtype, *usr, resource);
                printf("%s's permission = %d \n", resource, access);
            }
            else
            {
                CPermissionCheckThread** thrds = new CPermissionCheckThread*[numthrds];
                for(int i = 0; i < numthrds; i++)   
                    thrds[i] = new CPermissionCheckThread(secmgr, username, passwd, resource, rtype, numrounds);
                for(int j = 0; j < numthrds; j++)
                    thrds[j]->start();
                for(int k = 0; k < numthrds; k++)
                    thrds[k]->join();
            }
        }
        else if(stricmp(action, "-au") == 0)
        {
            if(username == NULL || *username == '\0')
            {
                printf("missing username\n");
                return -1;
            }

            Owned<ISecUser> usr = secmgr->createUser(username);
            if(firstname != NULL)
                usr->setFirstName(firstname);
            if(lastname != NULL)
                usr->setLastName(lastname);
            usr->credentials().setPassword(passwd);
            bool ok = secmgr->addUser(*usr);
            if(ok)
                printf("user %s added\n", username);
            else
                printf("user %s not added\n", username);
        }
        else if(stricmp(action, "-ar") == 0)
        {
            if(resource == NULL || *resource == '\0')
            {
                printf("missing resource\n");
                return -1;
            }

            SecResourceType rtype = RT_DEFAULT;
            if((resourcetype != NULL) && (stricmp(resourcetype, "filescope") == 0))
                rtype = RT_FILE_SCOPE;
            else if((resourcetype != NULL) && (stricmp(resourcetype, "workunit") == 0))
                rtype = RT_WORKUNIT_SCOPE;

            Owned<ISecUser> usr;
            if(username != NULL && *username != '\0')
                usr.setown(secmgr->createUser(username));

            bool ok = secmgr->addResourceEx(rtype, *usr, resource, PT_DEFAULT, NULL);
            if(!ok)
                printf("resource not added\n");
            else
                printf("resource %s added\n", resource);
        }
        else if(stricmp(action, "-cp") == 0)
        {
            if(username == NULL || *username == '\0')
            {
                printf("missing username\n");
                return -1;
            }
            StringBuffer passbuf, newpassbuf;
            if(passwd == NULL || *passwd == '\0')
            {
                getpassword("Enter password: ", passbuf, false);
                passwd = passbuf.str();
            }
            if(newpasswd == NULL || *newpasswd == '\0')
            {
                getpassword("\nEnter new password: ", newpassbuf, true);
                newpasswd = newpassbuf.str();
            }

            Owned<ISecUser> usr = secmgr->createUser(username);
            usr->credentials().setPassword(passwd);
            bool ok = secmgr->updateUser(*usr, newpasswd);
            if(ok)
                printf("user password changed\n");
            else
                printf("user password not changed\n");          
        }
#endif
    }
    catch(IException* e)
    {
        StringBuffer errmsg;
        e->errorMessage(errmsg);
        printf("%s\n", errmsg.str());
    }
    catch(...)
    {
        printf("Unknown exception\n");
    }

    releaseAtoms();

    return 0;
}
