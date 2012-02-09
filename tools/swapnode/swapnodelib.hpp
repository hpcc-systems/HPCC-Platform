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

#ifndef _SWAPNODE_LIB_HPP
#define _SWAPNODE_LIB_HPP

#ifdef _WIN32
 #ifdef SWAPNODELIB_EXPORTS
  #define swapnodelib_decl __declspec(dllexport)
 #else
  #define swapnodelib_decl __declspec(dllimport)
 #endif
#else
 #define swapnodelib_decl
#endif

interface IPropertyTree;
extern swapnodelib_decl bool swapNode(const char *cluster, const char *oldIP, const char *newIP);
extern swapnodelib_decl void emailSwap(const char *cluster, const char *msg, bool warn=false, bool sendswapped=false, bool sendhistory=false);

// Called from swapnode and thor
extern swapnodelib_decl bool checkThorNodeSwap(
                                      const char *cluster,      // which cluster
                                      const char *failedwuid,   // failed WUID or null if none
                                      unsigned mininterval=0    // minimal interval before redoing check (mins)
                                      ); // if returns true swap needed

#ifdef ENABLE_AUTOSWAP
extern swapnodelib_decl bool autoSwapNode(const char *cluster, bool dryrun);
extern swapnodelib_decl void swapNodeHistory(const char *cluster, unsigned days, StringBuffer *out);
extern swapnodelib_decl void swappedList(const char *cluster, unsigned days, StringBuffer *out);
#endif

#endif
