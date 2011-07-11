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

#ifndef rtldistr_incl
#define rtldistr_incl
#include "eclrtl.hpp"

struct IDistributionTable : public IInterface
{
    virtual void       report(StringBuffer &out)=0;
    virtual void       merge(MemoryBuffer &in)=0;
    virtual void       serialize(MemoryBuffer &out)=0;
};

struct IStringDistributionTable : public IDistributionTable
{
    virtual void       noteValue(unsigned len, const char *val)=0;
};

struct IRealDistributionTable : public IDistributionTable
{
    virtual void       noteValue(double val)=0;
};

struct IBoolDistributionTable : public IDistributionTable
{
    virtual void       noteValue(bool val)=0;
};

struct IIntDistributionTable : public IDistributionTable
{
    virtual void       noteValue(int val)=0;
};

struct IInt64DistributionTable : public IDistributionTable
{
    virtual void       noteValue(__int64 val)=0;
};

struct IUIntDistributionTable : public IDistributionTable
{
    virtual void       noteValue(unsigned int val)=0;
};

struct IUInt64DistributionTable : public IDistributionTable
{
    virtual void       noteValue(unsigned __int64 val)=0;
};


ECLRTL_API IStringDistributionTable *createIStringDistributionTable(const char *, unsigned size);
ECLRTL_API IRealDistributionTable *createIRealDistributionTable(const char *, unsigned size);
ECLRTL_API IBoolDistributionTable *createIBoolDistributionTable(const char *, unsigned size);
ECLRTL_API IIntDistributionTable *createIIntDistributionTable(const char *, unsigned size);
ECLRTL_API IInt64DistributionTable *createIInt64DistributionTable(const char *, unsigned size);
ECLRTL_API IUIntDistributionTable *createIUIntDistributionTable(const char *, unsigned size);
ECLRTL_API IUInt64DistributionTable *createIUInt64DistributionTable(const char *, unsigned size);

#endif
