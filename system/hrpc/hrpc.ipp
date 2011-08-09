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
