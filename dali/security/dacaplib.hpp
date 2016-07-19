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

#ifndef DACAPLIB_HPP
#define DACAPLIB_HPP

#include "jstring.hpp"
#include "dasess.hpp"

#ifdef DACAP_LINKED_IN
 // only use following routine linked in 
 // (i.e. not from the DLL which is seisint only)

extern  unsigned importDaliCapabilityXML_basic(const char *filename);
// this is the basic (single dali server node verification) import
// Note this routine  *does not* transfer data to Dali directly as dali can't start until it is imported!!
// This is inherently weak (there is no mechanism to ensure previous dali server node is removed) 
// We should adopt full client node authentication scheme if we want to ensure not broken

#define dacaplib_decl 
#endif

#ifndef dacaplib_decl
#define dacaplib_decl DECL_IMPORT
#endif


interface IDaliCapabilityCreator: extends IInterface
{
    virtual void setSystemID(const char *systemid)=0;
    virtual void setClientPassword(const char *password)=0;
    virtual void setServerPassword(const char *password)=0;
    virtual void addCapability(DaliClientRole role,const char *mac,const char *cpuid=NULL)=0;
    virtual void setLimit(DaliClientRole role,unsigned limit)=0;
    virtual void removeCapability(DaliClientRole role,const char *mac,const char *cpuid=NULL)=0;
    virtual void save(StringBuffer &text)=0;
    virtual void reset()=0;
};

extern  dacaplib_decl IDaliCapabilityCreator *createDaliCapabilityCreator();
extern  dacaplib_decl bool getMAC(const IpAddress &ip,StringBuffer &out);

#endif
