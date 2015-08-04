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

#ifndef HRPC_IPP
#define HRPC_IPP

#include "jexcept.hpp"

#define HRPCVERSION 1

//#define _HRPCTRACE 

IHRPC_Exception *MakeHRPCexception(int code);
IHRPC_Exception *MakeHRPCexception(int code,IException *e);
#ifdef _HRPCTRACE
void HRPCtrace(const char *fmt, ...) __attribute__((format(printf, 1, 2)));

#define THROWHRPCEXCEPTION(code) { HRPCtrace("\nRaising HRPC exception %d at %s line %d\n",code, __FILE__, __LINE__);\
   throw MakeHRPCexception(code); }
#define THROWHRPCEXCEPTION2(code,id) { HRPCtrace("\nRaising HRPC exception %d at %s line %d\n",code, __FILE__, __LINE__);\
   throw MakeHRPCexception(code,id); }
#define THROWHRPCEXCEPTIONEXC(code,exc) { StringBuffer str; \
   HRPCtrace("\nRaising HRPC exception %d at %s line %d [%s]\n",code, __FILE__, __LINE__, exc->errorMessage(str).str());\
   throw MakeHRPCexception(code,exc); }
#else
#define THROWHRPCEXCEPTION(code)    throw MakeHRPCexception(code)
#define THROWHRPCEXCEPTION2(code,id) throw MakeHRPCexception(code,id)
#define THROWHRPCEXCEPTIONEXC(code,exc) throw MakeHRPCexception(code,exc)
#endif

struct HRPCpacketheader // at the head of all HRPC packets
{
    unsigned size;          // packet size (from end of this)
    HRPCmoduleid module;    // to be improved
    enum packetflags
    {
        PFbigendian     =1,
        PFreturn        =2,
        PFexception     =4,
        PFcallback      =8,
        PFlocked        =0x10,

    };
#if defined(_WIN32) || defined(__linux__)
#define HRPCendian (HRPCpacketheader::PFbigendian)
#else
    #define HRPCendian 0
#endif

    unsigned char flags;
    unsigned char function; // limit of 256 functions per module (at present)
    unsigned char version;  


    static HRPCpacketheader* read(HRPCbuffer &b)  
    { 
        HRPCpacketheader* r=(HRPCpacketheader*)b.readptr(sizeof(HRPCpacketheader)); 
        if (r->version!=HRPCVERSION)
            THROWHRPCEXCEPTION(HRPCERR_mismatched_hrpc_version);        
        return r;
    }
    static HRPCpacketheader* write(HRPCbuffer &b)
    {
        b.ensure(sizeof(HRPCpacketheader));
        HRPCpacketheader* r=(HRPCpacketheader*)b.writeptr(sizeof(HRPCpacketheader));
        r->version=HRPCVERSION;
        return r;
    }
#if defined(_WIN32) || defined(__linux__)
    void winrev() 
    {
        _rev(size);
    }
#else
    void winrev() {}
#endif
};


#endif
