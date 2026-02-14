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

#include "udplib.hpp"
#include "udpsha.hpp"
#include "udptrs.hpp"
#include "udpipmap.hpp"

#include "jsocket.hpp"
#include "jlog.hpp"
#include "jencrypt.hpp"
#include "jsecrets.hpp"
#include "roxie.hpp"
#ifdef _WIN32
#include <winsock.h>
#else
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/resource.h>
#endif
#include <math.h>
#include <atomic>
#include <algorithm>

using roxiemem::DataBuffer;

class CMessagePacker : implements IMessagePacker, public CInterface
{
    static constexpr unsigned maxPackerBuffers = 8;  // Maximum buffers to pre-allocate in pool

    ISendManager   &parent;
    IUdpReceiverEntry &receiver;
    UdpPacketHeader package_header;
    DataBuffer     *part_buffer;
    const unsigned data_buffer_size;
    unsigned data_used;
    void *mem_buffer;
    unsigned mem_buffer_size;
    unsigned totalSize;
    bool            packed_request;
    MemoryBuffer    metaInfo;
    bool            last_message_done;
    int             queue_number;
    roxiemem::DataBufferAllocator<maxPackerBuffers> allocator;  // Bulk buffer allocator

public:
    IMPLEMENT_IINTERFACE;

    CMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, ISendManager &_parent, IUdpReceiverEntry &_receiver, const IpAddress & _sourceNode, unsigned _msgSeq, unsigned _queue, bool _encrypted)
        : parent(_parent), receiver(_receiver), data_buffer_size(DATA_PAYLOAD - sizeof(UdpPacketHeader) - (_encrypted ? 16 : 0)),
          allocator(bufferManager)

    {
        queue_number = _queue;

        package_header.length = 0;          // filled in with proper value later
        package_header.metalength = 0;
        package_header.ruid = ruid;
        package_header.msgId = msgId;
        package_header.pktSeq = 0;
        package_header.node.setIp(_sourceNode);
        package_header.msgSeq = _msgSeq;

        packed_request = false;
        
        // Allocate first buffer individually - defer bulk allocation until we know we need more
        part_buffer = bufferManager->allocate();
        
        assertex(data_buffer_size >= headerSize + sizeof(unsigned short));
        *(unsigned short *) (&part_buffer->data[sizeof(UdpPacketHeader)]) = headerSize;
        memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+sizeof(unsigned short)], messageHeader, headerSize);
        data_used = headerSize + sizeof(unsigned short);
        mem_buffer = 0;
        mem_buffer_size = 0;
        last_message_done = false;
        totalSize = 0;
    }

    ~CMessagePacker()
    {
        if (part_buffer)
            part_buffer->Release();
        // allocator destructor automatically releases remaining buffers
        if (mem_buffer) free (mem_buffer);
    }

    virtual void *getBuffer(unsigned len, bool variable) override
    {
        if (variable)
            len += sizeof(RecordLengthType);
        if (data_buffer_size < len)
        {
            // Won't fit in one, so allocate temp location
            if (mem_buffer_size < len)
            {
                free(mem_buffer);
                mem_buffer = checked_malloc(len, ROXIE_MEMORY_ERROR);
                mem_buffer_size = len;
            }
            packed_request = false;
            if (variable)
                return ((char *) mem_buffer) + sizeof(RecordLengthType);
            else
                return mem_buffer;
        }

        if (part_buffer && ((data_buffer_size - data_used) < len))
            flush(false); // Note that we never span records that are small enough to fit - this can result in significant wastage if record just over DATA_PAYLOAD/2

        if (!part_buffer)
        {
            part_buffer = allocator.allocate();
        }
        packed_request = true;
        if (variable)
            return &part_buffer->data[data_used + sizeof(UdpPacketHeader) + sizeof(RecordLengthType)];
        else
            return &part_buffer->data[data_used + sizeof(UdpPacketHeader)];
    }

    virtual void putBuffer(const void *buf, unsigned len, bool variable) override
    {
        if (variable)
        {
            assertex(len < MAX_RECORD_LENGTH);
            buf = ((char *) buf) - sizeof(RecordLengthType);
            *(RecordLengthType *) buf = len;
            len += sizeof(RecordLengthType);
        }
        totalSize += len;
        if (packed_request)
        {
            assert(len <= (data_buffer_size - data_used));
            data_used += len;
        }
        else
        {
            //Edge case - ensure that the length of a variable length record does not span a packet
            //this can occur when large rows > data_buffer_size are being appended to the buffer.
            if (variable && ((data_buffer_size - data_used) < sizeof(RecordLengthType)))
                flush(false);

            while (len)
            {
                if (!part_buffer)
                {
                    part_buffer = allocator.allocate();
                    data_used = 0;
                }
                unsigned chunkLen = data_buffer_size - data_used;
                if (chunkLen > len)
                    chunkLen = len;
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], buf, chunkLen);
                data_used += chunkLen;
                len -= chunkLen;
                buf = &(((char*)buf)[chunkLen]);
                if (len)
                    flush(false);
            }
        }
    }

    virtual void sendMetaInfo(const void *buf, unsigned len) override {
        metaInfo.append(len, buf);
    }

    virtual void flush() override { flush(true); }

    virtual unsigned size() const override
    {
        return totalSize;
    }
private:

    void flush(bool last_msg)
    {
        if (!last_message_done && last_msg)
        {
            last_message_done = true;
            if (!part_buffer)
            {
                part_buffer = allocator.allocate();
            }
            const char *metaData = metaInfo.toByteArray();
            unsigned metaLength = metaInfo.length();
            unsigned maxMetaLength = data_buffer_size - data_used;
            while (metaLength > maxMetaLength)
            {
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], metaData, maxMetaLength);
                put_package(part_buffer, data_used, maxMetaLength);
                metaLength -= maxMetaLength;
                metaData += maxMetaLength;
                data_used = 0;
                maxMetaLength = data_buffer_size;
                part_buffer = allocator.allocate();
            }
            if (metaLength)
                memcpy(&part_buffer->data[sizeof(UdpPacketHeader)+data_used], metaData, metaLength);
            package_header.pktSeq |= UDP_PACKET_COMPLETE;
            put_package(part_buffer, data_used, metaLength);
        }
        else if (part_buffer)
        {
            // Just flush current - used when no room for current row
            if (data_used)
                put_package(part_buffer, data_used, 0); // buffer released in put_package
            else
                part_buffer->Release(); // If NO data in buffer, release buffer back to pool
        }
        part_buffer = 0;
        data_used = 0;
    }

    void put_package(DataBuffer *dataBuff, unsigned datalength, unsigned metalength)
    {
        package_header.length = datalength + metalength + sizeof(UdpPacketHeader);
        package_header.metalength = metalength;
        memcpy(dataBuff->data, &package_header, sizeof(package_header));
        parent.writeOwn(receiver, dataBuff, package_header.length, queue_number);
        package_header.pktSeq++;
    }


};

extern UDPLIB_API IMessagePacker *createMessagePacker(ruid_t ruid, unsigned msgId, const void *messageHeader, unsigned headerSize, ISendManager &_parent, IUdpReceiverEntry &_receiver, const IpAddress & _sourceNode, unsigned _msgSeq, unsigned _queue, bool _encrypted)
{
    return new CMessagePacker(ruid, msgId, messageHeader, headerSize, _parent, _receiver, _sourceNode, _msgSeq, _queue, _encrypted);
}
