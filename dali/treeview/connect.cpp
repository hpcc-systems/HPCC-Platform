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



#include "connect.hpp"


#include "jlib.hpp"
#include "jfile.hpp"

#include "dasds.hpp"
#include "mpbase.hpp"
#include "mpcomm.hpp"
#include "daclient.hpp"

#include "util.hpp"

#define CONNECTION_TIMEOUT  10000


class Connection : public CInterface
{
protected:
    Connection_t type;
    IPropertyTree * pTree;
    LPSTR fname;

public:
    Connection()
    {
        type = CT_none;
        pTree = NULL;
        fname = NULL;
    }

    ~Connection()
    {
        if(fname) delete(fname);
    }
};


class LocalConnection : public Connection, implements IConnection
{
private: 
    IPropertyTree * loadTree(LPCSTR fname)
    {
        IPropertyTree * r = NULL;
        try
        {
            r = createPTreeFromXMLFile(fname);
        }
        catch(IException * e)
        {
            reportException(e);
        }
        catch(...)
        {
            showFIOErr(fname, true);
        }
        return r;
    }

public:
    IMPLEMENT_IINTERFACE;

    LocalConnection(LPCSTR filename) : Connection()
    {
        fname = strdup(filename);

        try
        {
            pTree = loadTree(fname);
            type = CT_local;
        }
        catch(IException * e)
        {
            reportException(e);
            e->Release();
            pTree = NULL;
            type = CT_none;
        }
    }       

    ~LocalConnection()
    {
        ::Release(pTree);
    }

    // IConnection
    virtual bool lockWrite() { return true; }
    virtual bool unlockWrite() { return true; }
    virtual Connection_t getType() { return type; }
    virtual void commit() { /* NULL */ }
    virtual IPropertyTree * queryRoot(const char * xpath) 
    { 
        if(xpath)
        {
            IPropertyTree * oldpTree = pTree;
            try
            {
                pTree = pTree->getPropTree(xpath);      // this links
                if(oldpTree) oldpTree->Release();           
            }
            catch(IException * e)
            {
                reportException(e);
                e->Release();
                pTree = oldpTree;                   
            }
        }
        return pTree; 
    }
    virtual LPCSTR queryName() { return type == CT_none ? NULL : fname; }
};


class RemoteConnection : public Connection, implements IConnection
{
private:
    IRemoteConnection * conn;
    StringBuffer pathToRoot;


    void killConnection()
    {
        if(pTree)
        {
            pTree->Release();
            pTree = NULL;
        }
        if(conn)
        {
            conn->commit();
            conn->Release();
            conn = NULL;
        }
    }

public:
    IMPLEMENT_IINTERFACE;

    RemoteConnection(SocketEndpointArray & epa) : Connection()
    {
        pathToRoot.append("/");
        fname = strdup("Remote Server");
        conn = NULL;
        try
        {
            IGroup * group = createIGroup(epa);
//          CSystemCapability capability(DCR_Diagnostic, "DALITREE");
//          capability.secure((byte *)CLIENT_ENCRYPT_KEY, strlen(CLIENT_ENCRYPT_KEY));
            if(initClientProcess(group, DCR_Util, 0, NULL, NULL, CONNECTION_TIMEOUT))//, &capability)) 
                type = CT_remote;
            group->Release();
        }
        catch(IException * e)
        {
            reportException(e);
            return;
        }
        catch(...)
        {
            MessageBox(NULL, "An error occured whilst attempting connection.", "Connection Error", MB_OK | MB_ICONEXCLAMATION);
            return;
        }
    }

    ~RemoteConnection()
    {       
        killConnection();
        closedownClientProcess();
    }

    // IConnection
    virtual bool lockWrite()
    {
        if (!conn) return false;
        try 
        {
            conn->changeMode(RTM_LOCK_WRITE, CONNECTION_TIMEOUT);
        }
        catch (ISDSException *e)
        {
            if (SDSExcpt_LockTimeout == e->errorCode())
            {
                e->Release();
                return false;
            }
            else
                throw e;
        }
        return true;
    }
    virtual bool unlockWrite()
    {
        if (!conn) return false;
        try 
        {
            conn->changeMode(0, CONNECTION_TIMEOUT);
        }
        catch (ISDSException *e)
        {
            if (SDSExcpt_LockTimeout == e->errorCode())
            {
                e->Release();
                return false;
            }
            else
                throw e;
        }
        return true;
    }
    virtual Connection_t getType() { return type; }
    virtual void commit() { if(conn) conn->commit(); }
    virtual IPropertyTree * queryRoot(const char * xpath) 
    { 
        killConnection();
        try
        {
            ISDSManager & sdsManager = querySDS();

            if(xpath) pathToRoot.append(xpath).append("/");
            conn = sdsManager.connect(pathToRoot.str(), myProcessSession(), 0, 5000);
        }
        catch(IException * e)
        {
            conn = NULL;
            type = CT_none;
            reportException(e);
            return NULL;            
        }
        pTree = conn->getRoot();
        type = CT_remote;
        return pTree; 
    }
    virtual LPCSTR queryName() { return type == CT_none ? NULL : fname; }
};



IConnection * createLocalConnection(LPCSTR filename)
{
    return new LocalConnection(filename);
}

IConnection * createRemoteConnection(SocketEndpointArray & epa)
{
    return new RemoteConnection(epa);
}
