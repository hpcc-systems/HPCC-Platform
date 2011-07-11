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

#ifndef DFUXREF_HPP
#define DFUXREF_HPP

#ifdef _WIN32
    #ifdef DFUXREFLIB_EXPORTS
        #define DFUXREFLIB_API __declspec(dllexport)
    #else
        #define DFUXREFLIB_API __declspec(dllimport)
    #endif
#else
    #define DFUXREFLIB_API
#endif

#include "XRefNodeManager.hpp"

#define PMtextoutput    0x01
#define PMtreeoutput    0x02
#define PMcsvoutput     0x04
#define PMbackupoutput  0x08
#define PMupdateeclwatch 0x10


extern  DFUXREFLIB_API IPropertyTree * runXRef(unsigned numclusters,const char **clusternames,IXRefProgressCallback *callback,unsigned numthreads);
extern  DFUXREFLIB_API IPropertyTree * RunProcess(unsigned nclusters,const char **clusters,unsigned numdirs,const char **dirbaselist,unsigned flags,IXRefProgressCallback *_msgcallback,unsigned numthreads);

extern  DFUXREFLIB_API IPropertyTree * runXRefCluster(const char *cluster,IXRefNode *nodeToUpdate);
// this will use sasha if enabled

extern  DFUXREFLIB_API void testGetDir();

#endif
