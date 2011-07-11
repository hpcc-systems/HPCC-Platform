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
#define dacaplib_decl __declspec(dllimport)
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
