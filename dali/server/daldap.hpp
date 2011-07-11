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

};

extern IDaliLdapConnection *createDaliLdapConnection(IPropertyTree *proptree);


#endif
