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

#ifndef DAFTDIR_IPP
#define DAFTDIR_IPP

#include "daftdir.hpp"
#include "ftbase.ipp"
#include "rmtpass.hpp"

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
    CachedPasswordProvider      passwordProvider;
    IRunningSlaveObserver &     observer;
    const char *                directory;
    Owned<INode>                node;
    Owned<IPropertyTree>        options;
    Owned<IPropertyTree>        resultTree;
    Semaphore *                 sem;
    LogMsgJobInfo               job;
};

#endif
