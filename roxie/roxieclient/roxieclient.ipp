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

#ifndef _ROXIECLIENT_IPP__
#define _ROXIECLIENT_IPP__

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>

#include "jlib.hpp"
#include "jsmartsock.hpp"
#include "jmisc.hpp"
#include "jqueue.tpp"
#include "jstream.hpp"

#include "roxieclient.hpp"


#define MAXBUFSIZE 0x8000

interface IReceivedRoxieException : extends IException
{
};

class ReceivedRoxieException: public CInterface, public IReceivedRoxieException
{
public:
    IMPLEMENT_IINTERFACE;

    ReceivedRoxieException(int code, const char *_msg) : errcode(code), msg(_msg) { };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append("ReceivedRoxieException: (").append(msg).append(")");
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };

private:
    int errcode;
    StringAttr msg;
};

interface IRoxieClientException : extends IException
{
};

class RoxieClientException: public CInterface, public IRoxieClientException
{
public:
    IMPLEMENT_IINTERFACE;

    RoxieClientException(int code, const char *_msg) : errcode(code), msg(_msg) { };
    int errorCode() const { return (errcode); };
    StringBuffer &  errorMessage(StringBuffer &str) const
    {
        return str.append("RoxieClientException: (").append(msg).append(")");
    };
    MessageAudience errorAudience() const { return (MSGAUD_user); };

private:
    int errcode;
    StringAttr msg;
};


class CDataInput : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CDataInput(const char* _name, int iw, IByteInputStream* _stream)
    {
        name.append(_name);
        record_width = iw;
        stream.set(_stream);
    }

    StringBuffer name;
    int          record_width;
    Linked<IByteInputStream> stream;
    Mutex        readMutex; 
};

class CDataOutput : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CDataOutput(const char* _name, int ow, IByteOutputStream* _stream)
    {
        name.append(_name);
        record_width = ow;
        stream.set(_stream);
    }

    StringBuffer name;
    int          record_width;
    Linked<IByteOutputStream> stream;
    Mutex        writeMutex;
};

class CDataInputCache : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;
    CDataInputCache(CDataInput* _input, unsigned _recordsPerQuery)
    {
        recordsPerQuery = _recordsPerQuery;
        inputLen = 0;
        curInput = 0;
        if(_input != NULL)
        {
            name.append(_input->name);
            input = _input;

            if(input->record_width == 0)
            {
                bytesPerQuery = 0;
                recordsPerBlock = 0;
            }
            else
            {
                bytesPerQuery = recordsPerQuery * _input->record_width;
                recordsPerBlock = BYTESPERBLOCK / _input->record_width;
                recordsPerBlock = (recordsPerBlock < 1)? 1: recordsPerBlock;
                buffer.reserve(bytesPerQuery);
            }
        }
    }

    int readInput();

    StringBuffer name;
    int          inputLen;
    int          curInput;
    StringBuffer buffer;
    unsigned     recordsPerQuery;
    unsigned     bytesPerQuery;
    unsigned     recordsPerBlock;
    CDataInput   *input;
};


class CDataOutputCache : public CInterface, implements IInterface
{
public:
    IMPLEMENT_IINTERFACE;

    CDataOutputCache(CDataOutput* _output)
    {
        isNonBlocked = false;

        if(_output != NULL)
        {
            name.append(_output->name);
            output = _output;
        }
    }
    virtual ~CDataOutputCache()
    {
        while (outputQ.ordinality() > 0)
            delete outputQ.dequeue();
    }

    int writeOutput();
    void clearOutput();
    int immediateOutput(size32_t len, const char* result);
    StringBuffer name;
    QueueOf<MemoryBuffer, false> outputQ;
    CDataOutput *output;
    bool isNonBlocked;
};

class CRoxieClient;

class RoxieThread : public Thread
{
private:
    Owned<ISmartSocket> roxieSock;
    StringBuffer query;

    CRoxieClient* roxieclient;
    
    Owned<CDataInputCache>  m_default_incache;
    Owned<CDataOutputCache> m_default_outcache;

    IArrayOf<CDataInputCache>  m_incaches;
    IArrayOf<CDataOutputCache> m_outcaches;
    
    unsigned m_recordsPerQuery;

    unsigned m_maxRetries;
    int      m_readTimeout;

public:
    RoxieThread(CRoxieClient* _roxieclient, const char *_query, unsigned recordsPerQuery, unsigned maxRetries, int readTimeout);

    void sendQuery();
    bool readInput();
    void writeOutput();
    void clearOutput();
    void immediateOutput(size32_t len, const char* result);
    void sendData(const char *reqbuf);
    void processQuery();
    int run();

    unsigned     bytesRead;
    unsigned     bytesWritten;
};

class CRoxieClient : public CInterface, implements IRoxieClient
{
public:
    IMPLEMENT_IINTERFACE;

    Mutex readMutex;    
    Mutex writeMutex;

    CRoxieClient(ISmartSocketFactory* socketfactory, int numThreads, int maxretries)
    {
        m_smartSocketFactory.set(socketfactory);
        m_numThreads = (numThreads < 1)? 1: ((numThreads > MAX_ROXIECLIENT_THREADS)? MAX_ROXIECLIENT_THREADS: numThreads);
        m_recordsPerQuery = DEFAULT_RECORDSPERQUERY;
        m_maxRetries = maxretries;
        m_readTimeout = 300;
    }

    virtual ~CRoxieClient()
    {
    }

    virtual ISmartSocketFactory* querySocketFactory()
    {
        return m_smartSocketFactory.get();
    }

    virtual void setInput(const char* id, unsigned in_width, IByteInputStream* istream)
    {
        if(istream != NULL)
        {
            if(id == NULL || id[0] == '\0')
            {
                m_default_input.setown(new CDataInput("", in_width, istream));
            }
            else
            {
                m_inputs.append(*(new CDataInput(id, in_width, istream)));
            }
        }
        else
        {
            ERRLOG("can't set input stream, it's NULL");
        }
    }
    
    virtual void setOutput(const char* resultName, unsigned out_width, IByteOutputStream* ostream)
    {
        if(ostream != NULL)
        {
            if(resultName == NULL || resultName[0] == '\0')
            {
                m_default_output.setown(new CDataOutput("", out_width, ostream));
            }
            else
            {
                m_outputs.append(*(new CDataOutput(resultName, out_width, ostream)));
            }
        }
        else
        {
            ERRLOG("can't set output stream, it's NULL");
        }
    }
    virtual void setRecordsPerQuery(unsigned recordsPerQuery)
    {
        m_recordsPerQuery = recordsPerQuery;
    }

    virtual void setMaxRetries(unsigned retries)
    {
        m_maxRetries = retries;
    }

    virtual void setReadTimeout(int readTimeout)
    {
        m_readTimeout = readTimeout;
    }

    virtual void runQuery(const char* query, bool trim);

private:
    Linked<ISmartSocketFactory> m_smartSocketFactory;
    unsigned                    m_numThreads;
    unsigned                    m_maxRetries;
    int                         m_readTimeout;

protected:
    IArrayOf<CDataInput>        m_inputs;
    IArrayOf<CDataOutput>       m_outputs;  
    Owned<CDataInput>           m_default_input;
    Owned<CDataOutput>          m_default_output;
    unsigned                    m_recordsPerQuery;

friend class RoxieThread;

};

#endif

