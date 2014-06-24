/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#ifndef __LAYOUTTRANS_HPP_
#define __LAYOUTTRANS_HPP_

#include "deffield.hpp"
#include "rtlkey.hpp"
#include "jhtree.hpp"
#include "thorhelper.hpp"

#ifdef _DEBUG
#define DEBUG_HELPERS_REQUIRED
#endif

class THORHELPER_API IRecordLayoutTranslator : public IInterface
{
public:
    class THORHELPER_API Failure : public CInterface
    {
    public:
        typedef enum { BadStructure = 1, MissingDiskField, UnkeyedDiskField, UntranslatableField, UnsupportedFilter } Code;
        
        virtual Code queryCode() const = 0;
        virtual StringBuffer & getDetail(StringBuffer & out) const = 0;
    };

    class SegmentMonitorContext : public IInterface, public IIndexReadContext
    {
    public:
        virtual unsigned ordinality() const = 0;
        virtual IKeySegmentMonitor * item(unsigned i) const = 0;
        virtual void reset() = 0;
    };

    class RowTransformContext : public CInterface
    {
    public:
        RowTransformContext(unsigned _num);
        ~RowTransformContext();
        void init(unsigned seq, unsigned num) { sizes[seq] = new unsigned[num]; ptrs[seq] = new byte const *[num]; }
        void set(unsigned seq, unsigned i, size32_t size, byte const * ptr) { sizes[seq][i] = size; ptrs[seq][i] = ptr; }
        void setFposIn(offset_t fpos) { fposIn = fpos; }

        size32_t querySize(unsigned seq, unsigned i) const { return sizes[seq][i]; }
        byte const * queryPointer(unsigned seq, unsigned i) const { return ptrs[seq][i]; }
        offset_t const * queryFposIn() const { return &fposIn; }
    private:
        unsigned num;
        size32_t * * sizes;
        byte const * * * ptrs;
        offset_t fposIn;
    };

    virtual bool querySuccess() const = 0;
    virtual Failure const & queryFailure() const = 0;
    virtual void checkSizes(char const * filename, size32_t activitySize, size32_t diskSize) const = 0;
    virtual bool queryKeysTransformed() const = 0;
    virtual SegmentMonitorContext * getSegmentMonitorContext() = 0;
    virtual void createDiskSegmentMonitors(SegmentMonitorContext const & in, IIndexReadContext & out) = 0;
    virtual RowTransformContext * getRowTransformContext() = 0;
    virtual size32_t transformRow(RowTransformContext * ctx, byte const * in, size32_t inSize, IMemoryBlock & out, offset_t & fpos) const = 0;
#ifdef DEBUG_HELPERS_REQUIRED
    virtual StringBuffer & getMappingsAsString(StringBuffer & out) const = 0;
#endif
};

extern THORHELPER_API IRecordLayoutTranslator * createRecordLayoutTranslator(IDefRecordMeta const * diskMeta, IDefRecordMeta const * activityMeta);
extern THORHELPER_API IRecordLayoutTranslator * createRecordLayoutTranslator(size32_t diskMetaSize, const void *diskMetaData, size32_t activityMetaSize, const void *activityMetaData);

class THORHELPER_API IRecordLayoutTranslatorCache : public IInterface
{
public:
    virtual IRecordLayoutTranslator * get(size32_t diskMetaSize, void const  * diskMetaData, size32_t activityMetaSize, void const * activityMetaData, IDefRecordMeta const * activityMeta = NULL) = 0; //if activityMeta is NULL, it is calculated by deserializing from buffer --- but option to supply so caller can deserialize once and use many
    virtual unsigned count() const = 0;
};

extern THORHELPER_API IRecordLayoutTranslatorCache * createRecordLayoutTranslatorCache();

#endif
