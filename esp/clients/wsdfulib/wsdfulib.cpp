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

#ifndef WSDFU_API
#ifdef _WIN32
#define WSDFU_API _declspec (dllexport)
#else
#define WSDFU_API
#endif
#endif

//Jlib
#include "jliball.hpp"

//SCM Interfaces
#include "ws_dfu.hpp"

//ESP Generated files
#include "ws_dfu_esp.ipp"

WSDFU_API IClientWsDfu * createWsDfuClient()
{
    return new CClientWsDfu;
}
