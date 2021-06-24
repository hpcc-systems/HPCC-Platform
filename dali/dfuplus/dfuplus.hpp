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
    int listhistory();
    int erasehistory();

    bool fixedSpray(const char* srcxml,const char* srcip,const char* srcfile,const char* srcplane,const MemoryBuffer &xmlbuf,const char* dstcluster,const char* dstname,const char *format,StringBuffer &retwuid,StringBuffer &except );
    bool variableSpray(const char* srcxml,const char* srcip,const char* srcfile,const char* srcplane,const MemoryBuffer &xmlbuf,const char* dstcluster,const char* dstname,const char *format,StringBuffer &retwuid,StringBuffer &except );

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
