/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef DASERVER_HPP
#define DASERVER_HPP


#include "jiface.hpp"
#include "mpbase.hpp"

interface IDaliServer: extends IInterface  // for all coven based servers
{
    virtual void start() = 0;
    virtual void ready() = 0; // called after all servers started
    virtual void suspend() = 0; // called before servers stopped.
    virtual void stop() = 0;
    virtual void nodeDown(rank_t rank) = 0;
    // more TBD
};

#define DALICONF "daliconf.xml"
#define DALICOVEN "dalicoven.xml"
extern Owned<IPropertyTree> serverConfig; //configuration properties

// server initialization
interface ICoven;
extern da_decl ICoven * initCoven(IGroup *grp,IPropertyTree *covenstore,const char *clientVersion=NULL,const char *minServerVersion=NULL);
extern da_decl void  covenMain(ICoven * coven);
extern da_decl void  closeCoven(ICoven * coven);

#endif
