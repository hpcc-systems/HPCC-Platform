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
#if !defined(AFX_DEPLOYUTILS_CONSTANTS_HPP__INCLUDED_)
#define AFX_DEPLOYUTILS_CONSTANTS_HPP__INCLUDED_
//---------------------------------------------------------------------------

#define BUILDSERVER        "\\\\brpsnt082c\\builds"

const char* const DEFAULT_BUILDNAME = "build_xxxx";
const char* const DEFAULT_BUILDSETNAME = "buildset";
const char* const DEFAULT_URL = BUILDSERVER"\\build_xxxx";
const char* const DEFAULT_PATH = "";
const char* const DEFAULT_INSTALLSET = "deploy_map.xml";
const char* const DEFAULT_MODULENAME   = "mod.lib";
const char* const DEFAULT_FUNCTIONNAME = "func";

//---------------------------------------------------------------------------
//  These must match the component icon bitmap
//---------------------------------------------------------------------------
enum COMPONENT_ICON { 
   ICON_UNKNOWN=0,
   ICON_FOLDERCLOSED,
   ICON_FOLDEROPEN,
   ICON_DEFAULT,
   ICON_ECLSERVER,
   ICON_ECLAGENT,
   ICON_HOLE,
   ICON_THOR,
   ICON_TOPOLOGY,
   ICON_PLUGIN,
   ICON_SYBASE,
   ICON_ESPSERVER,
   ICON_JOBSERVER,
   ICON_INSTANCE
};

//---------------------------------------------------------------------------
//  These must match the hole process icon bitmap
//---------------------------------------------------------------------------
enum HOLE_ROLE {
   HOLE_TOPO=0,
   HOLE_NONE,
   HOLE_CTRL,
   HOLE_SOCK,
   HOLE_COLL,
   HOLE_PROC,
   HOLE_STBY,
   HOLE_UNKNOWN
};

const char* const g_szHoleProcess[] =
{ "", "", "Control", "Socket", "Collator", "Processor", "Standby", "Unkown" };

//---------------------------------------------------------------------------
//  These must match the thor process icon bitmap
//---------------------------------------------------------------------------
enum THOR_ROLE {
   THOR_TOPO=0,
   THOR_NONE,
   THOR_MSTR,
   THOR_SLAV,
   THOR_SPAR,
   THOR_UNKNOWN
};

enum ROXIE_ROLE {
    ROXIE_TOPO=0,
   ROXIE_NONE,
   ROXIE_FARM,
   ROXIE_SERVER,
   ROXIE_SLAVE,
   ROXIE_UNKNOWN
};

const char* const g_szThorProcess[] =
{ "", "", "Master", "Slave", "Spare", "Unknown" };

const char* const g_szRoxieProcess[] =
{ "", "Farm", "Server", "Slave", "Unknown" };

//---------------------------------------------------------------------------
// 
//---------------------------------------------------------------------------
enum COMPUTER_STATE {
   STATE_UNAVAILABLE=0,
   STATE_AVAILABLE
};

const char* const g_szComputerState[] =
{ "Unavailable", "Available" };

const int COMPUTER_STATE_COUNT = sizeof(g_szComputerState) / sizeof (g_szComputerState[0]);

//---------------------------------------------------------------------------
#endif // !defined(AFX_DEPLOYUTILS_CONSTANTS_HPP__INCLUDED_)
