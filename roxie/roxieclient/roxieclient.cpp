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

#include "roxieclient.ipp"
#include "roxie.hpp"

#define MAXBUFSIZE 0x8000

#ifdef _DEBUG
//#define DEBUG_ROXIECLIENT_
#endif

int CDataInputCache::readInput()
{
    inputLen = 0;
    curInput = 0;
    synchronized procedure(input->readMutex);

    IByteInputStream* inputstream = input->stream.get();

    if(inputstream == NULL)
        return 0;

    int bytes = 0;
    if(input->record_width > 0)
    {
        char* inbuf = (char*)buffer.str();

        while ((inputLen < bytesPerQuery) && (bytes = inputstream->readBytes(inbuf + inputLen, bytesPerQuery - inputLen)) > 0)
        {
            inputLen += bytes;
        }
    }
    else
    {
        buffer.clear();
        char inbuf[2048];
        while ((bytes = inputstream->readBytes(inbuf, 2048)) > 0)
        {
            buffer.append(bytes, inbuf);
            inputLen += bytes;
        }
    }

    if (bytes < 0)
    {
        throw MakeStringException(-1, "Error in readInput(): errno = %d", errno);
    }

#ifdef DEBUG_ROXIECLIENT_
    if(input->record_width > 0)
        DBGLOG("Read %d bytes (%d records) from input stream %s", inputLen, inputLen/input->record_width, name.str());
    else
        DBGLOG("Read %d bytes from input stream %s", inputLen, name.str());
#endif

    return inputLen;
}

int CDataOutputCache::writeOutput()
{

    int bytes = 0;

    synchronized procedure(output->writeMutex);

    IByteOutputStream* outputstream = output->stream.get();
    if(outputstream == NULL)
        return 0;

    while (outputQ.ordinality() > 0)
    {
        MemoryBuffer *mb = outputQ.dequeue();
        size32_t len = mb->length();
        const char *obuf = mb->toByteArray();
        const char *finger = obuf;
        
        if(!isNonBlocked)
        {   
            finger = strchr(obuf+9, '\0') + 1;
            len -= (finger-obuf);
        }
        try {
            outputstream->writeBytes(finger, len);
        }
        catch (IException *e) {
            delete mb;
            throw;
        }

#ifdef DEBUG_ROXIECLIENT_
        DBGLOG("Written %d bytes to output %s", len, name.str());
#endif
        bytes += len;
        delete mb;
    }

    return bytes;
}

void CDataOutputCache::clearOutput()
{
    while (outputQ.ordinality() > 0)
        delete outputQ.dequeue();
}

int CDataOutputCache::immediateOutput(size32_t len, const char* result)
{
    synchronized procedure(output->writeMutex);

    IByteOutputStream* outputstream = output->stream.get();
    if(outputstream == NULL)
        return 0;
    
    if(result != NULL)
    {
        outputstream->writeBytes(result, len);
#ifdef DEBUG_ROXIECLIENT_
        DBGLOG("Written %d bytes to output %s", len, name.str());
#endif

        return len;
    }
    return 0;
}


RoxieThread::RoxieThread(CRoxieClient* _roxieclient, const char *_query, unsigned recordsPerQuery, unsigned maxRetries, int readTimeout) 
    : roxieclient(_roxieclient), query(_query), m_recordsPerQuery(recordsPerQuery)
{
    bytesRead = bytesWritten = 0;

    CDataInput* dinput = roxieclient->m_default_input.get();
    if(dinput != NULL)
    {
        m_default_incache.setown(new CDataInputCache(dinput, m_recordsPerQuery));
    }
    CDataOutput* doutput = roxieclient->m_default_output.get();
    if(doutput != NULL)
    {
        m_default_outcache.setown(new CDataOutputCache(doutput));
    }

    ForEachItemIn(i, roxieclient->m_inputs)
    {
        CDataInput* oneinput = &(roxieclient->m_inputs.item(i));
        if(oneinput != NULL)
        {
            m_incaches.append(*(new CDataInputCache(oneinput, m_recordsPerQuery)));
        }
    }

    ForEachItemIn(j, roxieclient->m_outputs)
    {
        CDataOutput* oneoutput = &(roxieclient->m_outputs.item(j));
        if(oneoutput != NULL)
        {
            m_outcaches.append(*(new CDataOutputCache(oneoutput)));
        }
    }
    
    m_maxRetries = maxRetries;
    m_readTimeout = readTimeout;
}

void RoxieThread::sendQuery()
{
    MemoryBuffer sendBuffer;

    unsigned queryLen = query.length();

    unsigned revQueryLen = queryLen;
    _WINREV(revQueryLen);
    sendBuffer.append(revQueryLen);
    sendBuffer.append(queryLen, query.str());

    roxieSock->write(sendBuffer.toByteArray(), sendBuffer.length());
#ifdef DEBUG_ROXIECLIENT_
    DBGLOG("Sent query %s", query.str());
#endif
}

bool RoxieThread::readInput()
{
    bool done = true;
    if(m_default_incache != NULL)
    {
        int len = m_default_incache->readInput();
        done = done && (len == 0);
        bytesRead += len;
    }
    
    ForEachItemIn(i, m_incaches)
    {
        CDataInputCache* oneincache = &(m_incaches.item(i));
        if(oneincache != NULL)
        {
            int len = oneincache->readInput();
            done = done && (len == 0);
            bytesRead += len;
        }
    }

    return done;
}

void RoxieThread::writeOutput()
{
    if(m_default_outcache != NULL)
    {
        int bytes = m_default_outcache->writeOutput();
        bytesWritten += bytes;
    }
    
    ForEachItemIn(i, m_outcaches)
    {
        CDataOutputCache* oneoutcache = &(m_outcaches.item(i));
        if(oneoutcache != NULL)
        {
            int bytes = oneoutcache->writeOutput();
            bytesWritten += bytes;
        }
    }

}

void RoxieThread::clearOutput()
{
    if(m_default_outcache != NULL)
        m_default_outcache->clearOutput();
    
    ForEachItemIn(i, m_outcaches)
    {
        CDataOutputCache* oneoutcache = &(m_outcaches.item(i));
        if(oneoutcache != NULL)
            oneoutcache->clearOutput();
    }

}

void RoxieThread::immediateOutput(size32_t len, const char* result)
{
    if(m_default_outcache != NULL)
    {
        m_default_outcache->immediateOutput(len, result);
        bytesWritten += len;
    }
    else
    {
        throw MakeStringException(-1, "Output stream not set. Pleaes set default outputstream, or use bxml as output format");
    }
}

void RoxieThread::sendData(const char *reqbuf)
{
    static unsigned sequenceNumber = 0;

    const char *inputName = reqbuf+9;
    int inputNameLen = strlen(inputName);
    
    CDataInputCache* icache = m_default_incache.get();

    ForEachItemIn(i, m_incaches)
    {
        CDataInputCache* oneincache = &(m_incaches.item(i));
        if(oneincache != NULL && strcmp(inputName, oneincache->name.str()) == 0)
        {
            icache = oneincache;
        }
    }

    if(icache == NULL)
        return;

    if (icache->curInput < icache->inputLen)
    {
        unsigned recordsLeft, numRecords, numBytes;
        int in_width = icache->input->record_width;
        if(in_width == 0)
        {
            numRecords = 0; // Wouldn't be able to know, so set to 0.
            numBytes = icache->inputLen;
        }
        else
        {
            recordsLeft = (icache->inputLen - icache->curInput) / in_width;
            numRecords = (recordsLeft < icache->recordsPerBlock)? recordsLeft: icache->recordsPerBlock;
            numBytes = numRecords * in_width;
        }

        MemoryBuffer sendBuffer;
        unsigned rev = numBytes + inputNameLen + 10;
        rev |= 0x80000000;
        _WINREV(rev);
        sendBuffer.append(rev);
        sendBuffer.append('R');
        rev = sequenceNumber++;
        _WINREV(rev);
        sendBuffer.append(rev);
        rev = numRecords;
        _WINREV(rev);
        sendBuffer.append(rev);
        sendBuffer.append(inputNameLen+1, inputName);
        sendBuffer.append(numBytes, icache->buffer.str()+icache->curInput);

        roxieSock->write(sendBuffer.toByteArray(), sendBuffer.length());
#ifdef DEBUG_ROXIECLIENT_
        DBGLOG("Wrote %d bytes from inputQ to socket", numBytes);
#endif
        icache->curInput += numBytes;
    }
    else
    {
        unsigned zeroLen = 0;
        roxieSock->write(&zeroLen, sizeof(zeroLen));
#ifdef DEBUG_ROXIECLIENT_
        DBGLOG("Wrote %d bytes to socket", sizeof(zeroLen));
#endif
    }
}

void RoxieThread::processQuery()
{
    char buf[MAXBUFSIZE];
    char *obuf;

    while (1)
    {
        bool blockedMsg = false;
        size32_t r = 0;

        roxieSock->read(buf,4,4,r, m_readTimeout);
        size32_t len = *(unsigned *)buf;
        _WINREV4(len);

        if (len & 0x80000000)
        {
            len ^= 0x80000000;
            blockedMsg = true;
        }

        if (len == 0)
            break;

        r=0;
        MemoryBuffer x;
        obuf = (char *) x.reserve(len);
        roxieSock->read(obuf, len, len, r, m_readTimeout);

        if (blockedMsg)
        {
            if (*obuf == 'D')
            {
                sendData(obuf);
            }
            else if (*obuf == 'R')
            {
                const char *finger = obuf+9;
                const char *payload = finger + strlen(finger) + 1;

                if (strcmp(finger, "Exception") == 0)
                {
                    StringBuffer body(x.length() - (payload-obuf), payload);
                    StringBuffer xml;
                    xml.append("<Exception>").append(body).append("</Exception>");
                    Owned<IPropertyTree> ep = createPTreeFromXMLString(xml.str());
                    int code = ep->getPropInt("./Code", 0);
                    SocketEndpoint peerEp;
                    StringBuffer peerStr;
                    DBGLOG("Connected to %s", roxieSock->querySocket()->getPeerEndpoint(peerEp).getUrlStr(peerStr).str());
                    DBGLOG("Roxie exception: %s", body.str());
                    throw new ReceivedRoxieException(code, body.str());
                }
                else
                {
                    CDataOutputCache* ocache = m_default_outcache.get();

                    ForEachItemIn(i, m_outcaches)
                    {
                        CDataOutputCache* oneoutcache = &(m_outcaches.item(i));
                        if(oneoutcache != NULL && strcmp(finger, oneoutcache->name.str()) == 0)
                        {
                            ocache = oneoutcache;
                        }
                    }
                    if(ocache != NULL)
                    {
                        int out_width = ocache->output->record_width;
                        if(out_width > 0 && (len - (payload-obuf)) % out_width != 0)
                        {
                            StringBuffer errmsg;
                            errmsg.appendf("received %d bytes of data, not a multiple of -ow %d", (unsigned) (len-(payload-obuf)), out_width);
                            throw new RoxieClientException(-1, errmsg.str());
                        }
                        MemoryBuffer* y = new MemoryBuffer;
                        y->setBuffer(len, x.detach(), true);
                        ocache->outputQ.enqueue(y);
                        continue;
                    }
                }
            }
        }
        else
        {
            CDataOutputCache* ocache = m_default_outcache.get();
            if(ocache != NULL)
            {
                ocache->isNonBlocked = true;
                MemoryBuffer* y = new MemoryBuffer;
                y->setBuffer(len, x.detach(), true);
                ocache->outputQ.enqueue(y);
            }
            //immediateOutput(x.length(), x.toByteArray());
        }
    }
}

int RoxieThread::run()
{
    bool done = false;
    unsigned attempts;
    int retryInterval;
    Owned<IRandomNumberGenerator> random = createRandomNumberGenerator();
    random->seed((unsigned)get_cycles_now());
    
    while (!done)
    {
        try
        {
            done = readInput();
        }
        catch (IException *E)
        {
            EXCLOG(E, "readInput");
            throw MakeStringException(-1, "Error readInput");
        }
        catch (...)
        {
            throw MakeStringException(-1, "Unknown exception in readInput");
        }

        attempts = 0;
        retryInterval = 1;

        while(1)
        {
            try
            {
                roxieSock.setown(roxieclient->querySocketFactory()->connect_timeout(9000));

                SocketEndpoint peerEp;
                StringBuffer peerStr;
                DBGLOG("Connected to %s", roxieSock->querySocket()->getPeerEndpoint(peerEp).getUrlStr(peerStr).str());

                sendQuery();
                processQuery();

                break;
            }
            catch (ISmartSocketException *e)
            {
                StringBuffer errmsg;
                e->errorMessage(errmsg);
                ERRLOG("Fatal exception: %s", errmsg.str());
                StringBuffer buf;
                buf.append("<Exception><Source>roxieclient</Source><Message>").append(errmsg.str()).append("</Message></Exception>");
                immediateOutput(buf.length(), buf.str());
                clearOutput();
                e->Release();
                return 0;
            }
            catch (IReceivedRoxieException *e)
            {
                if (e->errorCode() == ROXIE_TOO_MANY_QUERIES) // server busy exception
                {
                    int sleepTime = retryInterval + random->next() % retryInterval;
                    DBGLOG("Thread sleeping for %d seconds", sleepTime);
                    Sleep(sleepTime*1000);
                    retryInterval = (retryInterval*2 >= 30)? 30: retryInterval*2;
                }
                else
                {
                    StringBuffer errmsg;
                    e->errorMessage(errmsg);
                    DBGLOG("Roxieclient received exception from Roxie: %s", errmsg.str());
                    if (attempts < m_maxRetries)
                    {
                        DBGLOG("Retrying (%d times out of maxRetries %d)", attempts, m_maxRetries);
                        e->Release();
                        attempts++;
                    }
                    else
                    {
                        DBGLOG("Returning: maxRetries exceeded");
                        StringBuffer buf("<Exception><Source>roxieclient</Source><Message>");
                        buf.append(errmsg.str()).append(" and Roxie maxRetries exceeded</Message></Exception>");
                        immediateOutput(buf.length(), buf.str());
                        clearOutput();
                        e->Release();
                        return 0;
                    }
                }

                clearOutput();
                e->Release();
            }
            catch (IRoxieClientException* e)
            {
                StringBuffer errmsg;
                e->errorMessage(errmsg);
                DBGLOG("Roxieclient caught exception: %s", errmsg.str());
                StringBuffer buf("<Exception><Source>roxieclient</Source><Message>");
                buf.append(errmsg.str()).append("</Message></Exception>");
                immediateOutput(buf.length(), buf.str());
                clearOutput();
                e->Release();
                return 0;
            }
            catch (IException *e)
            {
                StringBuffer errmsg;
                e->errorMessage(errmsg);
                DBGLOG("Roxieclient caught exception: %s", errmsg.str());
                if (attempts < m_maxRetries)
                {
                    DBGLOG("Retrying (%d times out of maxRetries %d)", attempts, m_maxRetries);
                    e->Release();
                    attempts++;
                }
                else
                {
                    DBGLOG("Returning: maxRetries exceeded");
                    StringBuffer buf("<Exception><Source>roxieclient</Source><Message>");
                    buf.append(errmsg.str()).append(" and Roxie maxRetries exceeded</Message></Exception>");
                    immediateOutput(buf.length(), buf.str());
                    clearOutput();
                    e->Release();
                    return 0;
                }
            }
            catch(...)
            {
                ERRLOG("Caught unexpected Roxie exception");
                StringBuffer buf("<Exception><Source>roxieclient</Source><Message>Unexpected exception</Message></Exception>");
                immediateOutput(buf.length(), buf.str());
                clearOutput();
                return 0;
            }
        }

        writeOutput();

#ifdef DEBUG_ROXIECLIENT_
        DBGLOG("Thread progress: %d bytes read, %d  bytes written", bytesRead, bytesWritten);
#endif
    }

    return 0;
}

void CRoxieClient::runQuery(const char* query, bool trim)
{
    TIME_SECTION("CRoxieClient::runQuery");
    IArrayOf<RoxieThread> threads;

    if(!query || !query)
        throw MakeStringException(-1, "query is empty");

    StringBuffer qxml;
    if(trim)
    {
        Owned<IPropertyTree> qtree = createPTreeFromXMLString(query);
        if(qtree)
        {
            qtree->setPropBool("@trim", true);
            toXML(qtree, qxml);
        }
        else
            qxml.append(query);
    }

    int i;

    for (i=0; i< m_numThreads; i++)
    {
        RoxieThread* onethread = new RoxieThread(this, trim?qxml.str():query, m_recordsPerQuery, m_maxRetries, m_readTimeout);
        threads.append(*onethread);
        DBGLOG("Starting thread %d", i);
        onethread->start();
    }

    unsigned totalRead = 0;
    unsigned totalWritten = 0;

    for (i=0; i< m_numThreads; i++)
    {
        DBGLOG("Waiting for thread %d to finish", i);
        RoxieThread* onethread = &threads.item(i);
        if(onethread == NULL)
            continue;
        onethread->join();
        DBGLOG("Final stats for thread %d: %d bytes read, %d bytes written", i, onethread->bytesRead, onethread->bytesWritten);
        totalRead += onethread->bytesRead;
        totalWritten += onethread->bytesWritten;
    }

    DBGLOG("Final roxieclient stats: %d bytes read, %d bytes written", totalRead, totalWritten);
}

extern "C" ROXIECLIENT_API IRoxieClient* createRoxieClient(ISmartSocketFactory* socketfactory, int numThreads, int maxretries)
{
    return new CRoxieClient(socketfactory, numThreads, maxretries);
}

