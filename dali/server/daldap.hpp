/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef DALDAP_HPP
#define DALDAP_HPP

interface IUserDescriptor;

#define DLF_ENABLED     0x01
#define DLF_SAFE        0x02
#define DLF_SCOPESCANS  0x04

interface IDaliLdapConnection: extends IInterface
{
    virtual int getPermissions(const char *key,const char *obj,IUserDescriptor *udesc,unsigned auditflags)=0;
    virtual bool checkScopeScans() = 0;
    virtual unsigned getLDAPflags() = 0;
    virtual void setLDAPflags(unsigned flags) = 0;
    virtual bool clearPermissionsCache(IUserDescriptor *udesc) = 0;
    virtual bool enableScopeScans(IUserDescriptor *udesc, bool enable, int *err) = 0;
};

extern IDaliLdapConnection *createDaliLdapConnection(IPropertyTree *proptree);


#endif
