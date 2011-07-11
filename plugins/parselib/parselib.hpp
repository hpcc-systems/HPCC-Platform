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

#ifndef PARSELIB_INCL
#define PARSELIB_INCL

#ifdef _WIN32
#define PARSELIB_CALL _cdecl
#ifdef PARSELIB_EXPORTS
#define PARSELIB_API __declspec(dllexport)
#else
#define PARSELIB_API __declspec(dllimport)
#endif
#else
#define PARSELIB_CALL
#define PARSELIB_API
#endif

#include "hqlplugins.hpp"

extern "C" {
PARSELIB_API bool getECLPluginDefinition(ECLPluginDefinitionBlock *pb);
PARSELIB_API void setPluginContext(IPluginContext * _ctx);
PARSELIB_API void plGetDefaultParseTree(IMatchWalker * walker, unsigned & len, char * & text);
PARSELIB_API void plGetXmlParseTree(IMatchWalker * walker, unsigned & len, char * & text);
}

#endif
