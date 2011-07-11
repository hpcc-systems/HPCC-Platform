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
#ifndef _HQLPLUGININFO_HPP_
#define _HQLPLUGININFO_HPP_

#include "hql.hpp"
#include "jptree.hpp"

namespace repositoryCommon {

extern HQL_API IEclRepository * loadPlugins(const char * pluginPath, const char * libraryPath, const char * dynamicPath);
extern HQL_API IPropertyTree * createPluginPropertyTree(IEclRepository * plugins, bool includeModuleText);
extern HQL_API IPropertyTree * getPlugin(IPropertyTree * p, IEclRepository * plugins, const char* modname, bool load);

}   //namespace

#endif
