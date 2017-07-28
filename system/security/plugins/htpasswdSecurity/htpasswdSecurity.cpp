/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems®.

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

#include "basesecurity.hpp"
#include "authmap.ipp"
#include <apr_md5.h>
#include "htpasswdSecurity.hpp"

class CHtpasswdSecurityManager : public CBaseSecurityManager
{
public:
    CHtpasswdSecurityManager(const char *serviceName, IPropertyTree *secMgrCfg, IPropertyTree *bindConfig) : CBaseSecurityManager(serviceName, (IPropertyTree *)NULL)
	{
        if (secMgrCfg)
            pwFile.set(secMgrCfg->queryProp("@htpasswdFile"));
        if(pwFile.isEmpty())
            throw MakeStringException(-1, "htpasswdFile not found in configuration");

        {
            Owned<IPropertyTree> authcfg = bindConfig->getPropTree("Authenticate");
            if(authcfg != nullptr)
            {
                StringBuffer authxml;
                toXML(authcfg, authxml);
                DBGLOG("HTPASS Authenticate Config: %s", authxml.str());
            }
        }

        {
            Owned<IPropertyTree> custombindingconfig = bindConfig->getPropTree("CustomBindingParameters");
            if(custombindingconfig != nullptr)
            {
                StringBuffer custconfigxml;
                toXML(custombindingconfig, custconfigxml);
                DBGLOG("HTPASS Custom Binding Config: %s", custconfigxml.str());
            }
        }

        apr_initialized = false;
	}

	~CHtpasswdSecurityManager()
	{
		userMap.kill();
	}

	secManagerType querySecMgrType()
	{
		return SMT_HTPasswd;
	}

	inline virtual const char* querySecMgrTypeName() { return "htpasswd"; }

	IAuthMap * createAuthMap(IPropertyTree * authconfig)
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
						rlist = createResourceList("htpasswdsecurity");
						authmap->add(pathstr.str(), rlist);
					}
					ISecResource* rs = rlist->addResource(rstr.str());
                    SecAccessFlags requiredaccess = str2perm(required.str());
					rs->setRequiredAccessFlags(requiredaccess);
					rs->setDescription(description.str());
                    rs->setAccessFlags(SecAccess_Full);//grant full access to authenticated users
				}
				loc_iter->next();
			}
		}

		return authmap;
	}

protected:

    //ISecManager
	bool IsPasswordValid(ISecUser& sec_user)
	{
		StringBuffer user;
		user.append(sec_user.getName());
		if (0 == user.length())
			throw MakeStringException(-1, "htpasswd User name is NULL");

		if (sec_user.credentials().getSessionToken().length())//Already authenticated it token exists
		    return true;

		CriticalBlock block(crit);
		if (!apr_initialized)
			initAPR();
		loadPwds();//reload password file if modified
		StringBuffer *encPW = userMap.getValue(user.str());
		if (encPW && encPW->length())
		{
			apr_status_t rc = apr_password_validate(sec_user.credentials().getPassword(), encPW->str());
			if (rc != APR_SUCCESS)
				DBGLOG("htpasswd authentication for user %s failed - APR RC %d", user.str(), rc );
			return rc == APR_SUCCESS;
		}
		DBGLOG("User %s not in htpasswd file", user.str());
		return false;
	}

    const char * getDescription() override
    {
        return "HTPASSWD Security Manager";
    }

    bool authorize(ISecUser & user, ISecResourceList * resources, IEspSecureContext* secureContext) override
    {
        return IsPasswordValid(user);
    }

    unsigned getPasswordExpirationWarningDays() override
    {
        return -2;//never expires
    }

    SecAccessFlags authorizeEx(SecResourceType rtype, ISecUser & user, const char * resourcename, IEspSecureContext* secureContext) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

    SecAccessFlags getAccessFlagsEx(SecResourceType rtype, ISecUser& sec_user, const char* resourcename) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

    SecAccessFlags authorizeFileScope(ISecUser & user, const char * filescope) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

    bool authorizeViewScope(ISecUser & user, ISecResourceList * resources)
    {
        int nResources = resources->count();
        for (int ri = 0; ri < nResources; ri++)
        {
            ISecResource* res = resources->queryResource(ri);
            if(res != nullptr)
            {
                assertex(res->getResourceType() == RT_VIEW_SCOPE);
                res->setAccessFlags(SecAccess_Full);//grant full access to authenticated users
            }
        }
        return true;//success
    }

    SecAccessFlags authorizeWorkunitScope(ISecUser & user, const char * filescope) override
    {
        return SecAccess_Full;//grant full access to authenticated users
    }

private:

	void initAPR()
	{
		try
		{
			apr_status_t rc = apr_md5_init(&md5_ctx);
			if (rc != APR_SUCCESS)
				throw MakeStringException(-1, "htpasswd apr_md5_init returns error %d", rc );
			apr_initialized = true;
		}
		catch (...)
		{
			throw MakeStringException(-1, "htpasswd exception calling apr_md5_init");
		}
	}

	bool loadPwds()
	{
		try
		{
			if (!pwFile.length())
				throw MakeStringException(-1, "htpasswd Password file not specified");

			Owned<IFile> file = createIFile(pwFile.str());
			if (!file->exists())
			{
				userMap.kill();
				throw MakeStringException(-1, "htpasswd Password file does not exist");
			}

			bool isDir;
			offset_t size;
			CDateTime whenChanged;
			file->getInfo(isDir,size,whenChanged);
			if (isDir)
			{
				userMap.kill();
				throw MakeStringException(-1, "htpasswd Password file specifies a directory");
			}
			if (0 == whenChanged.compare(pwFileLastMod))
				return true;//Don't reload if file unchanged
			userMap.kill();
			OwnedIFileIO io = file->open(IFOread);
			if (!io)
				throw MakeStringException(-1, "htpasswd Unable to open Password file");

			MemoryBuffer mb;
			size32_t count = read(io, 0, (size32_t)-1, mb);
			if (0 == count)
				throw MakeStringException(-1, "htpasswd Password file is empty");

			mb.append((char)NULL);
			char * p = (char*)mb.toByteArray();
			char *saveptr;
			const char * seps = "\f\r\n";
			char * next = strtok_r(p, seps, &saveptr);
			if (next)
			{
				do
				{
					char * colon = strchr(next,':');
					if (NULL == colon)
						throw MakeStringException(-1, "htpasswd Password file appears malformed");
					*colon = (char)NULL;
					userMap.setValue(next, colon+1);//username, enctypted password
					next = strtok_r(NULL, seps, &saveptr);
				} while (next);
			}

			io->close();
			pwFileLastMod = whenChanged;//remember when last changed
		}
		catch(IException*)
		{
			throw MakeStringException(-1, "htpasswd Exception accessing Password file");
		}
		return true;
	}


private:
	mutable CriticalSection crit;
	StringBuffer    pwFile;
	CDateTime       pwFileLastMod;
	bool            apr_initialized;
	MapStringTo<StringBuffer> userMap;
	apr_md5_ctx_t 	md5_ctx;
};

extern "C"
{
    HTPASSWDSECURITY_API ISecManager * createInstance(const char *serviceName, IPropertyTree &secMgrCfg, IPropertyTree &bndCfg)
    {
        return new CHtpasswdSecurityManager(serviceName, &secMgrCfg, &bndCfg);
    }

}

