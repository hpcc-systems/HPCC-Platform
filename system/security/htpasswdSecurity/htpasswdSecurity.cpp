/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

#pragma warning( disable : 4786 )

#include "defaultsecuritymanager.hpp"
#include "authmap.ipp"
#include "htpasswdsecurity.hpp"

//========htpasswd Security Manager==========

CHtpasswdSecurityManager::CHtpasswdSecurityManager(const char *serviceName, IPropertyTree *authconfig) : CDefaultSecurityManager(serviceName, (IPropertyTree *)NULL)
{
    if (authconfig)
        authconfig->getProp("@htpasswdFile", pwFile);
}

CHtpasswdSecurityManager::~CHtpasswdSecurityManager()
{
    users.kill();
    passwords.kill();
}

void CHtpasswdSecurityManager::loadAprLib()
{
    try
    {
/*
        aprLib.setown( newAprObject());
        if (aprLib)
            aprLib->init();
*/
    }
    catch (...)
    {
        throw MakeStringException(-1, "htpasswd Cannot load dynapr library");
    }
}

bool CHtpasswdSecurityManager::IsPasswordValid(ISecUser& sec_user)
{
    StringBuffer user;
    user.append(sec_user.getName());
    if (0 == user.length())
        throw MakeStringException(-1, "htpasswd User name is NULL");

    CriticalBlock block(crit);
    loadPwds();//reload password file if modified
    for (aindex_t x=0; x<users.length(); x++)
    {
        if (strlen(users.item(x)) == strlen(user.str()) && 0 == strcmp(users.item(x), user.str()))
        {
/*
            if (!aprLib)
                loadAprLib();
*/
            StringBuffer userPW(sec_user.credentials().getPassword());
            StringBuffer encPW(passwords.item(x));
/*
            apr_ret_t rc = aprLib->apr_md5_validate(userPW, encPW);
            return rc == APR_MD5_TRUE;
*/
            return true;
        }
    }
    return false;
}

bool CHtpasswdSecurityManager::loadPwds()
{
    try
    {
        if (!pwFile.length())
            throw MakeStringException(-1, "htpasswd Password file not specified");

        Owned<IFile> file = createIFile(pwFile.str());
        if (!file->exists())
        {
            users.kill();
            passwords.kill();
            throw MakeStringException(-1, "htpasswd Password file '%s' does not exist", pwFile.str());
        }

        bool isDir;
        offset_t size;
        CDateTime whenChanged;
        file->getInfo(isDir,size,whenChanged);
        if (isDir)
        {
            users.kill();
            passwords.kill();
            throw MakeStringException(-1, "htpasswd Password file '%s' specifies a directory", pwFile.str());
        }
        if (0 == whenChanged.compare(pwfileLastMod))
            return true;//Don't reload if file unchanged
        users.kill();
        passwords.kill();

        OwnedIFileIO io = file->open(IFOread);
        if (!io)
            throw MakeStringException(-1, "htpasswd Unable to open Password file '%s'", pwFile.str());

        MemoryBuffer mb;
        size32_t count = read(io, 0, (size32_t)-1, mb);
        if (0 == count)
            throw MakeStringException(-1, "htpasswd Password file '%s' is empty", pwFile.str());

        mb.append((char)NULL);
        char * p = (char*)mb.toByteArray();
        char *saveptr;
        const char * seps = "\f\r\n";
        char * next = strtok_r(p, seps, &saveptr);
        if (next)
        {
            do
            {
                //Populate users and passwords arrays
                char * colon = strchr(next,':');
                if (NULL == colon)
                    throw MakeStringException(-1, "htpasswd Password file '%s' appears malformed", pwFile.str());
                *colon = (char)NULL;
                users.append(next);
                passwords.append(colon+1);
            } while (next = strtok_r(NULL, seps, &saveptr));
        }

        io->close();
        pwfileLastMod = whenChanged;//remember when last changed
    }
    catch(IException*)
    {
        throw MakeStringException(-1, "htpasswd Exception accessing Password file '%s'", pwFile.str());
    }
    return true;
}

IAuthMap * CHtpasswdSecurityManager::createAuthMap(IPropertyTree * authconfig)
{
    CAuthMap* authmap = new CAuthMap(this);

    Owned<IPropertyTreeIterator> loc_iter;
    loc_iter.setown(authconfig->getElements(".//Location"));
    if (loc_iter)
    {
        IPropertyTree *location = NULL;
        loc_iter->first();
        while(loc_iter->isValid())
        {
            location = &loc_iter->query();
            if (location)
            {
                StringBuffer pathstr, rstr, required, description;
                location->getProp("@path", pathstr);
                location->getProp("@resource", rstr);
                location->getProp("@required", required);
                location->getProp("@description", description);

                if(pathstr.length() == 0)
                    throw MakeStringException(-1, "path empty in Authenticate/Location");
                if(rstr.length() == 0)
                    throw MakeStringException(-1, "resource empty in Authenticate/Location");

                ISecResourceList* rlist = authmap->queryResourceList(pathstr.str());
                if(rlist == NULL)
                {
                    rlist = createResourceList("localsecurity");
                    authmap->add(pathstr.str(), rlist);
                }
                ISecResource* rs = rlist->addResource(rstr.str());
                unsigned requiredaccess = str2perm(required.str());
                rs->setRequiredAccessFlags(requiredaccess);
                rs->setDescription(description.str());
            }
            loc_iter->next();
        }
    }

    return authmap;
}

extern "C"
{
    HTPASSWDSECURITY_API ISecManager * newHtpasswdSecManager(const char *serviceName, IPropertyTree &config)
    {
        return new CHtpasswdSecurityManager(serviceName, &config);
    }
}

