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
