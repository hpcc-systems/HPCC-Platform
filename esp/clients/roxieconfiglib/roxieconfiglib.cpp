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

#pragma warning(disable : 4786)

#ifdef WIN32
#define ws_roxieconfig_API _declspec (dllexport)
#else
#define ws_roxieconfig_API
#endif

//Jlib
#include "jliball.hpp"
#include "xslprocessor.hpp"

//SCM Interfaces
#include "ws_roxieconfig.hpp"

//ESP Generated files
#include "ws_roxieconfig_esp.ipp"

ws_roxieconfig_API IClientws_roxieconfig * createws_roxieconfigClient()
{
    return new CClientws_roxieconfig;
}
