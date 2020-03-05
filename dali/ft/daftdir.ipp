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

#ifndef DAFTDIR_IPP
#define DAFTDIR_IPP

#include "daftdir.hpp"
#include "ftbase.ipp"

class DirectoryBuilder : public CInterface
{
public:
    DirectoryBuilder(ISocket * _masterSocket, IPropertyTree * options);

    void rootDirectory(const char * directory, INode * node, IPropertyTree * result);
    bool walkDirectory(const char * path, IPropertyTree * directory);

protected:
    ISocket *   masterSocket;
    bool        addTimes;
    bool        calcCRC;
    bool        recurse;
    bool        implicitWildcard;
    bool        includeEmptyDirectory;
    StringAttr  wildcard;
};

class DirectoryCopier : public CInterface
{
public:
    DirectoryCopier(ISocket * _masterSocket, MemoryBuffer & in);
    DirectoryCopier(ISocket * _masterSocket, IPropertyTree * _source, RemoteFilename & _target, IPropertyTree * _options);

    void copy();

protected:
    void initOptions();
    void recursiveCopy(IPropertyTree * level, const char * sourcePath, const char * targetPath);

protected:
    ISocket * masterSocket;
    Owned<IPropertyTree> source;
    RemoteFilename target;
    Owned<IPropertyTree> options;
    bool onlyCopyMissing;
    bool onlyCopyExisting;
    bool preserveTimes;
    bool preserveIfNewer;
    bool verbose;
};

//----------------------------------------------------------------------------

interface IRunningSlaveObserver
{
public:
    virtual void addSlave(ISocket * node) = 0;
    virtual void removeSlave(ISocket * node) = 0;
};

class DirectoryThread : public Thread
{
public:
    DirectoryThread(IRunningSlaveObserver & _observer, const char * _directory, INode * _node, IPropertyTree * _options);

            void go(Semaphore & _sem);
            IPropertyTree * getTree() { return resultTree.getClear(); }         // returns ownership
    virtual int run();

public:
    Linked<IException>          error;
    bool                        ok;

protected:
    bool performCommand();
    bool commandAndSignal();

protected:
    IRunningSlaveObserver &     observer;
    const char *                directory;
    Owned<INode>                node;
    Owned<IPropertyTree>        options;
    Owned<IPropertyTree>        resultTree;
    Semaphore *                 sem;
    LogMsgJobInfo               job;
};

#endif
