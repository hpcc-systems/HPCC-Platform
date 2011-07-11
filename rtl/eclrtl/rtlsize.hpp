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

#ifndef hosize_incl
#define hosize_incl

#ifdef _WIN32
#ifdef ECLRTL_EXPORTS
#define ECLRTL_API __declspec(dllexport)
#else
#define ECLRTL_API __declspec(dllimport)
#endif
#else
#define ECLRTL_API
#endif
#include "rtltype.hpp"


class OffsetInfoArray;
class OffsetInfoBase;

class SizeSelector
{
public:
    SizeSelector(OffsetInfoBase & _monitor);
    ~SizeSelector();

    unsigned getSize();
    unsigned getOffset();

    SizeSelector & reset();

protected:
    OffsetInfoBase & monitor;
};


class OffsetInfoBase
{
public:
    OffsetInfoBase(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize);

    virtual void addChild(OffsetInfoBase & child);
            SizeSelector & get();
            unsigned getOffset();
            unsigned getSize();
    virtual void moveSection(unsigned offset, int delta);
    virtual bool isValid();
//  virtual void structureChanged(unsigned * oldSize, unsigned * newSize);

protected:
    virtual void adjustSize(int delta);

protected:
    OffsetInfoBase * owner;
    OffsetInfoBase * pred;
    unsigned predDelta;
    SizeSelector curSelection;
    unsigned fixedSize;
    unsigned cachedSize;
};

class CompoundOffsetInfo : public OffsetInfoBase
{
public:
    CompoundOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize);
    ~CompoundOffsetInfo();

    void addChild(OffsetInfoBase & child);

protected:
    OffsetInfoArray * children;
};


class RecordOffsetInfo : public CompoundOffsetInfo
{
public:
    RecordOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize);
    ~RecordOffsetInfo();
};


class RootRecordOffsetInfo : public CompoundOffsetInfo
{
public:
    RootRecordOffsetInfo(const void * _baseAddress, unsigned _fixedSize);

    virtual void moveSection(unsigned offset, int delta);
protected:
    unsigned char * baseAddress;
};


class IfBlockOffsetInfo : public CompoundOffsetInfo
{
public:
    IfBlockOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize, bool _isOkay);

    virtual void adjustSize(int delta);
    virtual bool isValid();
            void setValid(bool nowValid);

protected:
    bool isOkay;
};


class AlienOffsetInfo : public OffsetInfoBase
{
public:
    AlienOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _startSize); // or calculation function);

    void set(unsigned len, void * data);
};



//############################################################################################################################################################
//############################################################################################################################################################
//############################################################################################################################################################
#if 0
class SizeChildArray;

/*
Helps with the following:
1. What is the size of a given field
2. What is the offset of a given field.
3. Be told when ifblock conditions/dimensions change
4. Relocate variables that have moved when dimensions/if blocks change
   - this is done one change at a time....
5. default initialise fields when allocated.

* Problems:
1. How is the reallocation done.
2. How is the record reinitialised.
3. How are isValid/Dimensions initialised when higher if/arrays cause creation/destruction.

*/


class SizeSelector
{
public:
    SizeSelector(SizeMonitor & _monitor, unsigned maxDimensions);

    unsigned getSize();
    unsigned getOffset();

    SizeSelector & reset();
    SizeSelector & reset(unsigned numDims, unsigned * dims);
    SizeSelector & select(unsigned idx);

protected:
    SizeMonitor & monitor;
    unsigned curDimension;
    unsigned maxDimensions;
    unsigned * curSelection;
};


class SizeMonitor
{
public:
    SizeMonitor(SizeMonitor * owner, SizeMonitor * pred, unsigned predDelta);

    SizeSelector & get();
    void structureChanged();

protected:
    virtual void gatherDimensions(unsigned * tgt, unsigned * dimensions) = 0;
    virtual unsigned getOffset(unsigned * dimensions) = 0;
    virtual unsigned getSize(unsigned * dimensions) = 0;
    virtual void isValid(unsigned * dimensions) = 0;
//  virtual void structureChanged(unsigned * oldSize, unsigned * newSize);

protected:
    SizeMonitor * owner;
    SizeMonitor * pred;
    unsigned predDelta;
    unsigned maxDimensions;
    SizeSelector curSelection;
};

class RecordMonitor
{
public:
    RecordMonitor(SizeMonitor * owner, SizeMonitor * pred, unsigned predDelta, unsigned fixedSize);
    ~RecordMonitor();

    void addChild(SizeMonitor & child);

protected:
    SizeChildArray * children;
};


class ArrayMonitor : public RecordMonitor
{
public:
    ArrayMonitor(SizeMonitor * owner, SizeMonitor * pred, unsigned predDelta, unsigned fixedSize, unsigned numDims);

    unsigned * curDimensions;   // n dimensional
    unsigned * newDimensions;
};

class IfBlockMonitor : public RecordMonitor
{
public:
    RecordMonitor(SizeMonitor * owner, SizeMonitor * pred, unsigned predDelta, unsigned fixedSize, bool isValid);
};


class AlienMonitor : public SizeMonitor
{
public:
    AlienMonitor(SizeMonitor * owner, SizeMonitor * pred, unsigned predDelta, unsigned startSize) // or calculation function);

    void set(unsigned len, void * data);
};


/*
{
    RecordSelector r1(NULL, NULL, 100);
    ArraySelector a1(r1, NULL, d, DefaultDimensions); 
    RecordSelector r2(&a1, NULL, 5);
    RecordSelector r3(&a1, &r2, 5);
    ArraySelector a2

    r3.get().select(3).getSize();
    a1.resize(
};
*/
#endif

#endif
