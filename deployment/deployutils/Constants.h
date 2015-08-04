/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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
