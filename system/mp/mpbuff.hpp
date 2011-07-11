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

#ifndef MPBUFF_HPP
#define MPBUFF_HPP

#ifndef mp_decl
#define mp_decl __declspec(dllimport)
#endif

#include "jutil.hpp"
#include "jsocket.hpp"
#include "jlog.hpp"
#include "mpbase.hpp"
#include "mptag.hpp"

class mp_decl CMessageBuffer: public MemoryBuffer
{

    SocketEndpoint  sender;
    mptag_t         tag;
    mptag_t         replytag;

public:
    CMessageBuffer() : MemoryBuffer() { init(); }
    CMessageBuffer(size32_t initsize) : MemoryBuffer(initsize) { init(); }
    CMessageBuffer(size32_t len, const void * buffer) : MemoryBuffer(len,buffer) { init(); }

    inline const SocketEndpoint &getSender() const  { return sender; }
    inline void setReplyTag(mptag_t tag)            { replytag = tag; }  // called prior to send (use cresteReplyTag to make tag)
    inline mptag_t getReplyTag()                    { return replytag; } // called after recv to determine tag to reply to
    inline mptag_t getTag()                     { return tag; }      

    inline void init()             
    { 
        tag = TAG_NULL;
        replytag = TAG_NULL;
    }   

    inline void init(const SocketEndpoint &_sender, mptag_t _tag, mptag_t _replytag)
    {
        sender = _sender;
        tag = _tag;
        replytag = _replytag;
    }

    void transferFrom(CMessageBuffer &mb)
    {
        // endian TBD
        swapWith(mb);
        tag = mb.tag;
        sender = mb.sender;
        replytag = mb.replytag;
        mb.clear();
    }

    StringBuffer &getDetails(StringBuffer &buf)
    {
        StringBuffer ep;
        StringBuffer data;
        unsigned n=(length()<16)?length():16;
        for (unsigned i=0;i<n;i++) {
            if (i!=0) 
                data.append(", ");
            data.append((unsigned)(byte)toByteArray()[i]);
        }
        return buf.appendf("CMessageBuffer(%8X) tag=%d, sender=%s, replytag=%d, size== %d, data head = %s", (int)(long)toByteArray(), (int)tag, sender.getUrlStr(ep).str(), (int)replytag, length(), data.str());
    }

    CMessageBuffer *clone()
    {
        // copies data (otherwise use transferFrom)
        CMessageBuffer *ret = new CMessageBuffer();
        ret->tag = tag;
        ret->sender = sender;
        ret->replytag = replytag;
        ret->append(length(),toByteArray());
        return ret;
    }


};

#endif
