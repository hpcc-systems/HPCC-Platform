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

#ifndef _LDAPSECURITY_HPP__
#define _LDAPSECURITY_HPP__

#ifndef LDAPSECURITY_API

#ifdef _WIN32
    #ifndef LDAPSECURITY_EXPORTS
        #define LDAPSECURITY_API __declspec(dllimport)
    #else
        #define LDAPSECURITY_API __declspec(dllexport)
    #endif //LDAPSECURITY_EXPORTS
#else
    #define LDAPSECURITY_API
#endif //_WIN32

#endif 

extern "C" LDAPSECURITY_API ISecManager * newLdapSecManager(const char *serviceName, IPropertyTree &config);
extern "C" LDAPSECURITY_API ISecManager * newDefaultSecManager(const char *serviceName, IPropertyTree &config);
extern "C" LDAPSECURITY_API ISecManager * newLocalSecManager(const char *serviceName, IPropertyTree &config);
extern "C" LDAPSECURITY_API IAuthMap *newDefaultAuthMap(IPropertyTree* config);

#endif
