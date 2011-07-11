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
extern da_decl void  initCoven(IGroup *grp,IPropertyTree *covenstore,const char *clientVersion=NULL,const char *minServerVersion=NULL);
extern da_decl void  covenMain();
extern da_decl void  closeCoven();

#endif
