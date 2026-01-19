/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2025 HPCC Systems®.

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

#ifndef JIOURING_HPP
#define JIOURING_HPP

#include <vector>
#include "jptree.hpp"

#if defined(__linux__) || defined(__APPLE__) || defined(EMSCRIPTEN)
#include <sys/uio.h>
#else
//Define the iovec structure for windows/
struct iovec {
    void   *iov_base;  /* Starting address */
    size_t  iov_len;   /* Size of the memory pointed to by iov_base. */
};
#endif

interface IAsyncProcessor;
interface IAsyncCallback
{
    virtual void onAsyncComplete(int result) = 0;
};

//-----------------------------------------------------------------------------------------------

struct sockaddr;
interface IAsyncProcessor : public IInterface
{
// Functions for each of the asynchronous actions that can be queued
    virtual void enqueueCallbackCommand(IAsyncCallback & callback) = 0;
    virtual void enqueueCallbackCommands(const std::vector<IAsyncCallback *> & callbacks) = 0;
    
    // Enqueue an asynchronous socket connect operation
    // socket: The socket to connect (must be created with createForAsyncConnect)
    // addr: Socket address to connect to (caller must free with free() after calling this)
    // addrlen: Length of the socket address structure
    // callback: Will be called when connect completes (result = 0 for success, negative error code for failure)
    virtual void enqueueSocketConnect(ISocket * socket, const struct sockaddr * addr, size32_t addrlen, IAsyncCallback & callback) = 0;
    
    virtual void enqueueSocketRead(ISocket * socket, void * buf, size32_t len, IAsyncCallback & callback) = 0;
    virtual void enqueueSocketWrite(ISocket * socket, const void * buf, size32_t len, IAsyncCallback & callback) = 0;
    virtual void enqueueSocketWriteMany(ISocket * socket, const iovec * buffers, unsigned numBuffers, IAsyncCallback & callback) = 0;
    
    // Enqueue a multishot accept operation that continuously accepts connections
    // socket: Listening socket to accept connections on
    // callback: Will be called for each accepted connection (result = file descriptor for new connection)
    //           Also called once with -ECANCELED when the operation is cancelled
    //           IMPORTANT: The callback object must remain valid until the operation is cancelled
    //           or the processor is terminated. Caller is responsible for ensuring callback lifetime.
    virtual void enqueueSocketMultishotAccept(ISocket * socket, IAsyncCallback & callback) = 0;
    
    // Cancel an active multishot accept operation
    // socket: The listening socket to cancel multishot accept for
    virtual void cancelMultishotAccept(ISocket * socket) = 0;

// Functions for managing the completion queue - particularly non-threaded urings
    virtual void checkForCompletions() = 0;
    virtual void lockMemory(const void * buffer, size_t len) = 0; // Avoid memcpy by locking the memory
    virtual void terminate() = 0;

    virtual bool supportsMultishotAccept() const = 0;
};

// Create an instance of the IAsyncProcessor interface - may return null if not supported
extern jlib_decl IAsyncProcessor * createURingProcessor(const IPropertyTree * config, bool threaded);

// Helper that checks expert/@useIOUring configuration before creating URing processor
// Returns null if disabled via configuration or if not supported on platform
extern jlib_decl IAsyncProcessor * createURingProcessorIfEnabled(const IPropertyTree * config, bool threaded);

#endif
