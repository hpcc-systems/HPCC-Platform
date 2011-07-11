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

#ifndef _DFUPLUS_HPP__
#define _DFUPLUS_HPP__

#include "jliball.hpp"
#include "ws_fs.hpp"
#include "ws_dfu.hpp"

typedef MapStringTo<int> MapStrToInt;

interface CDfuPlusMessagerIntercept
{
    virtual void info(const char *msg)=0;
    virtual void err(const char *msg)=0;
};

class CDfuPlusHelper : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE
    
    CDfuPlusHelper(IProperties* _globals,   CDfuPlusMessagerIntercept *_msgintercept=NULL);
    virtual ~CDfuPlusHelper();

    int doit();

    CDfuPlusMessagerIntercept *msgintercept;
    bool runLocalDaFileSvr(SocketEndpoint &listenep,bool requireauthenticate=false,unsigned timeout=INFINITE); // only supported in dfuplus command line
    bool checkLocalDaFileSvr(const char *eps,SocketEndpoint &epout); // only supported in dfuplus command line

private:
    int spray();
    int replicate();
    int despray();
    int copy();
    int remove();
    int rename();
    int list();
    int recover();
    int superfile(const char* action);
    int savexml();
    int add();
    int status();
    int abort();
    int resubmit();
    int monitor();
    int rundafs();
    int copysuper();
    int updatejobname(const char* wuid, const char* jobname);

    bool fixedSpray(const char* srcxml,const char* srcip,const char* srcfile,const MemoryBuffer &xmlbuf,const char* dstcluster,const char* dstname,const char *format,StringBuffer &retwuid,StringBuffer &except );
    bool variableSpray(const char* srcxml,const char* srcip,const char* srcfile,const MemoryBuffer &xmlbuf,const char* dstcluster,const char* dstname,const char *format,StringBuffer &retwuid,StringBuffer &except );

    int waitToFinish(const char* wuid);
    void info(const char *format, ...) __attribute__((format(printf, 2, 3)));
    void error(const char *format, ...) __attribute__((format(printf, 2, 3)));
    void progress(const char *format, ...) __attribute__((format(printf, 2, 3)));
    void exc(const IMultiException &e,const char *title);

    Owned<IProperties> globals;
    Owned<IClientFileSpray> sprayclient;
    Owned<IClientWsDfu> dfuclient;
    MapStrToInt encodingmap;

    Owned<Thread> dafsthread;
};

#endif
