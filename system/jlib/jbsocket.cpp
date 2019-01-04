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


#include "jlog.hpp"
#include "jsocket.hpp"

#define BSOCKET_BUFSIZE 1024

class BufferedSocket : implements IBufferedSocket, public CInterface
{
private:
    char m_buf[BSOCKET_BUFSIZE + 1];
    unsigned short m_endptr;
    unsigned short m_curptr;
    unsigned int m_timeout;

    ISocket* m_socket;

public:
    IMPLEMENT_IINTERFACE;

    BufferedSocket(ISocket* socket);

    virtual int readline(char* buf, int maxlen, IMultiException *me)
    { return readline(buf, maxlen, false, me); }
    virtual int readline(char* buf, int maxlen, bool keepcrlf, IMultiException *me);

    //always make the size of buf at lease maxlen+1
    virtual int read(char* buf, int maxlen);
    virtual void setReadTimeout(unsigned int timeout)
    { m_timeout = timeout;  }
};


BufferedSocket::BufferedSocket(ISocket* socket)
{
    m_timeout = BSOCKET_READ_TIMEOUT;
    
    if(socket == NULL)
    {
        throw MakeStringException(-1, "can't create BufferedSocket from NULL socket");
    }
    
    m_socket = socket;
    
    m_endptr = 0;
    m_curptr = 0;
};

//always make the size of buf at lease maxlen+1
int BufferedSocket::readline(char* buf, int maxlen, bool keepcrlf, IMultiException *me)
{
    if(maxlen <= 0)
        return 0;

    int ptr = 0;

    try
    {
        while(ptr < maxlen)
        {
            bool foundCRLF = false;
            while(ptr < maxlen && m_curptr < m_endptr)
            {
                if(m_buf[m_curptr] == '\r') // standard case, \r\n marks a new header line.
                {
                    m_curptr++;
                    
                    if(keepcrlf)
                        buf[ptr++] = '\r';

                    foundCRLF = true;
                    //Skip \n
                    if(m_curptr < m_endptr)
                    {
                        if(m_buf[m_curptr] == '\n')
                        {
                            m_curptr++;
                            if(keepcrlf)
                                buf[ptr++] = '\n';
                        }
                    }
                    else
                    {
                        m_curptr = 0;
                        m_endptr = 0;
                        unsigned readlen;
                        m_socket->read(m_buf, 0, BSOCKET_BUFSIZE, readlen, m_timeout);
                        if(readlen > 0)
                        {
                            m_endptr = readlen;
                            if(m_buf[m_curptr] == '\n')
                            {
                                m_curptr++;
                                if(keepcrlf)
                                    buf[ptr++] = '\n';
                            }
                        }
                        
                    }
                    break;
                }
                else if(m_buf[m_curptr] == '\n') // deal with non-standard case, when only a \n marks a new line.
                {
                    m_curptr++;
                    if(keepcrlf)
                        buf[ptr++] = '\n';
                    foundCRLF = true;
                    break;
                }

                buf[ptr++] = m_buf[m_curptr++];
            }
            
            if(foundCRLF)
                break;

            // If no data left, read more
            if(m_curptr == m_endptr)
            {
                m_curptr = 0;
                m_endptr = 0;
                unsigned readlen;
                m_socket->read(m_buf, 0, BSOCKET_BUFSIZE, readlen, m_timeout);
                if(readlen <= 0)
                    break;
                m_endptr = readlen;
            }
        }
    }
    catch (IException *e) 
    {
        StringBuffer estr;
        int ret = -1;

        switch (e->errorCode())
        {
            //expected:
            case JSOCKERR_not_opened:            //= -1,    // accept,name,peer_name,read,write
            case JSOCKERR_broken_pipe:           //= -4,    // read,write
            case JSOCKERR_timeout_expired:       //= -6,    // read
            case JSOCKERR_graceful_close:        //= -10    // read,send
            {
                DBGLOG("socket(%d) : Exception(%d, %s)", m_socket->OShandle(), e->errorCode(), e->errorMessage(estr).str());
                buf[ptr] = 0;
                ret = ptr;
                break; //these errors should not be trapped as errors, but need to be logged.
            }
        
            //unexpected:
            case JSOCKERR_ok:
            case JSOCKERR_bad_address:           //= -2,    // connect
            case JSOCKERR_connection_failed:     //= -3,    // connect
            case JSOCKERR_invalid_access_mode:   //= -5,    // accept
            case JSOCKERR_port_in_use:           //= -7,    // create
            case JSOCKERR_cancel_accept:         //= -8,    // accept
            case JSOCKERR_connectionless_socket: //= -9,    // accept, cancel_accept
            default:
            {
                OERRLOG("In BufferedSocket::readline() -- Exception(%d, %s) reading from socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket->OShandle());
                break;
            }
        }

        if (me)
            me->append(*e);
        else
            e->Release();
        return ret;
    }
    catch(...)
    {
        OERRLOG("In BufferedSocket::readline() -- Unknown exception reading from socket(%d).", m_socket->OShandle());
        return -1;
    }
    
    buf[ptr] = 0;
    return ptr;
}

//always make the size of buf at lease maxlen+1
int BufferedSocket::read(char* buf, int maxlen)
{
    if(maxlen <= 0)
        return 0;

    int ptr = 0;
    while(ptr < maxlen)
    {
        while(ptr < maxlen && m_curptr < m_endptr)
        {
            buf[ptr++] = m_buf[m_curptr++];
        }
        
        if(ptr >= maxlen)
            break;

        // If no data left, read more
        if(m_curptr == m_endptr)
        {
            m_curptr = 0;
            m_endptr = 0;
            unsigned readlen;
            try
            {
                m_socket->read(m_buf, 0, BSOCKET_BUFSIZE, readlen, m_timeout);
            }
            catch (IException *e) 
            {
                StringBuffer estr;
                int ret = -1;

                switch (e->errorCode())
                {
                    case JSOCKERR_graceful_close:        //= -10    // read,send
                    {
                        buf[ptr] = 0;
                        ret = ptr;
                        break;
                    }

                    //expected:
                    case JSOCKERR_not_opened:            //= -1,    // accept,name,peer_name,read,write
                    case JSOCKERR_broken_pipe:           //= -4,    // read,write
                    case JSOCKERR_timeout_expired:       //= -6,    // read
                    {
                        DBGLOG("socket(%d) : Exception(%d, %s)", m_socket->OShandle(), e->errorCode(), e->errorMessage(estr).str());
                        buf[ptr] = 0;
                        ret = ptr;
                        break;
                    }
                
                    //unexpected:
                    case JSOCKERR_ok:
                    case JSOCKERR_bad_address:           //= -2,    // connect
                    case JSOCKERR_connection_failed:     //= -3,    // connect
                    case JSOCKERR_invalid_access_mode:   //= -5,    // accept
                    case JSOCKERR_port_in_use:           //= -7,    // create
                    case JSOCKERR_cancel_accept:         //= -8,    // accept
                    case JSOCKERR_connectionless_socket: //= -9,    // accept, cancel_accept
                    default:
                    {
                        OERRLOG("In BufferedSocket::readline() -- Exception(%d, %s) reading from socket(%d).", e->errorCode(), e->errorMessage(estr).str(), m_socket->OShandle());
                        break;
                    }
                }

                e->Release();
                return ret;
            }
            catch(...)
            {
                OERRLOG("In BufferedSocket::read() -- Unknown exception reading from socket(%d).", m_socket->OShandle());
                return -1;
            }

            if(readlen <= 0)
                break;
            m_endptr = readlen;
        }
    }
    buf[ptr] = 0;
    return ptr;
}

IBufferedSocket* createBufferedSocket(ISocket* socket)
{
    return new BufferedSocket(socket);
}
