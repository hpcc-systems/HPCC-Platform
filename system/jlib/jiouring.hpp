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

interface IAsyncProcessor;
interface IAsyncCallback
{
    virtual void onAsyncComplete(int result) = 0;
};

//-----------------------------------------------------------------------------------------------
interface IAsyncProcessor : public IInterface
{
// Functions for each of the asynchronous actions that can be queued
    virtual void enqueueCallbackCommand(IAsyncCallback & callback) = 0;
    virtual void enqueueCallbackCommands(std::vector<IAsyncCallback *> callbacks) = 0;
    virtual void enqueueSocketWrite(ISocket * socket, size32_t len, const void * buf, IAsyncCallback & callback) = 0;

// Functions for managing the completion queue - particularly non-threaded urings
    virtual void checkForCompletions() = 0;
    virtual void lockMemory(const void * buffer, size_t len) = 0; // Avoid memcpy by locking the memory
    virtual void terminate() = 0;
};

// Create an instance of the IAsyncProcessor interface - may return null if not supported
extern jlib_decl IAsyncProcessor * createURingProcessor(const IPropertyTree * config, bool threaded);

#endif
