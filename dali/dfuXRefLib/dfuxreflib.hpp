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

#ifndef DFUXREF_HPP
#define DFUXREF_HPP

#ifdef DFUXREFLIB_EXPORTS
    #define DFUXREFLIB_API DECL_EXPORT
#else
    #define DFUXREFLIB_API DECL_IMPORT
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
