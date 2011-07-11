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



#ifndef __JSET__
#define __JSET__

#include "jexpdef.hpp"



interface jlib_decl IBitSet : public IInterface 
{
    virtual void set(unsigned n,bool val=true)      = 0;
    virtual bool invert(unsigned n)                 = 0;            // returns inverted value
    virtual bool test(unsigned n)                   = 0;
    virtual bool testSet(unsigned n,bool val=true)  = 0;            // returns prev val
    virtual unsigned scan(unsigned from,bool tst)       = 0;        // returns index of first = val >= from
    virtual unsigned scanInvert(unsigned from,bool tst) = 0;        // like scan but inverts bit as well
    virtual void incl(unsigned lo, unsigned hi)     = 0;
    virtual void excl(unsigned lo, unsigned hi)     = 0;
    virtual void reset() = 0;
    virtual void serialize(MemoryBuffer &buffer) const = 0;
};

extern jlib_decl IBitSet *deserializeIBitSet(MemoryBuffer &mb);

// Simple BitSet // 0 based, all intermediate items exist, operations threadsafe and atomic
extern jlib_decl IBitSet *createBitSet(); 




#endif
