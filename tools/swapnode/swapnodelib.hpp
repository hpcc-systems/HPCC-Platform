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

#ifndef _SWAPNODE_LIB_HPP
#define _SWAPNODE_LIB_HPP

#ifdef SWAPNODELIB_EXPORTS
 #define swapnodelib_decl DECL_EXPORT
#else
 #define swapnodelib_decl DECL_IMPORT
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
