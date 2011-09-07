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

#include "platform.h"
#include <math.h>
#include <stdio.h>
#include "jmisc.hpp"
#include "jutil.hpp"
#include "jlib.hpp"
#include "eclrtl.hpp"

#include "rtlsize.hpp"

/*************** This file is not yet used.  WIP ******************************/
class OffsetInfoArray : public PointerArray
{
public:
    OffsetInfoBase & item(unsigned idx) { return *(OffsetInfoBase *)PointerArray::item(idx); };
};


SizeSelector::SizeSelector(OffsetInfoBase & _monitor) : monitor(_monitor)
{
}

SizeSelector::~SizeSelector()
{
}

unsigned SizeSelector::getSize()
{
    if (monitor.isValid())
        return monitor.getSize();
    return 0;
}

SizeSelector & SizeSelector::reset()
{
    return *this;
}

//---------------------------------------------------------------------------

#ifdef _MSC_VER
#pragma warning (push)
#pragma warning( disable : 4355 )
#endif
OffsetInfoBase::OffsetInfoBase(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize) : curSelection(*this)
{
    owner = _owner;
    pred = _pred;
    predDelta = _predDelta;
    fixedSize = _fixedSize;
    cachedSize = fixedSize;
}
#ifdef _MSC_VER
#pragma warning (pop)
#endif

void OffsetInfoBase::addChild(OffsetInfoBase & child)
{
    assertex(false);
}

void OffsetInfoBase::adjustSize(int delta)
{
    if (owner)
        owner->adjustSize(delta);
    cachedSize += delta;
}

SizeSelector & OffsetInfoBase::get()
{
    return curSelection.reset();
}


unsigned OffsetInfoBase::getOffset()
{
    if (pred)
        return pred->getOffset() + pred->getOffset() + predDelta;
    if (owner)
        return owner->getOffset() + predDelta;
    return 0;
}


unsigned OffsetInfoBase::getSize()
{
    return cachedSize;
}

void OffsetInfoBase::moveSection(unsigned offset, int delta)
{
    owner->moveSection(offset, delta);
}

bool OffsetInfoBase::isValid()
{
    if (owner)
        return owner->isValid();
    return true;
}



//---------------------------------------------------------------------------

CompoundOffsetInfo::CompoundOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize) : OffsetInfoBase(_owner, _pred, _predDelta, _fixedSize)
{
    children = new OffsetInfoArray;
}

CompoundOffsetInfo::~CompoundOffsetInfo()
{
    delete children;
}


void CompoundOffsetInfo::addChild(OffsetInfoBase & child)
{
    children->append(&child);
    if (child.isValid())
        adjustSize(child.getSize());
}

//---------------------------------------------------------------------------

RecordOffsetInfo::RecordOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize) :
    CompoundOffsetInfo(_owner, _pred, _predDelta, _fixedSize)
{
    if (owner)
        owner->addChild(*this);
}


//---------------------------------------------------------------------------

RootRecordOffsetInfo::RootRecordOffsetInfo(const void * _baseAddress, unsigned _fixedSize) :
    CompoundOffsetInfo(NULL, NULL, 0, _fixedSize)
{
    baseAddress = (unsigned char *)_baseAddress;
}


void RootRecordOffsetInfo::moveSection(unsigned offset, int delta)
{
    if (delta > 0)
    {
        memmove(baseAddress+offset+delta, baseAddress+offset, cachedSize-offset);
        memset(baseAddress+offset, 0xff, delta);
    }
    else
    {
        memmove(baseAddress+offset, baseAddress+offset-delta, cachedSize-offset+delta);
    }
}



//---------------------------------------------------------------------------

IfBlockOffsetInfo::IfBlockOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _fixedSize, bool _isOkay) :
    CompoundOffsetInfo(_owner, _pred, _predDelta, _fixedSize)
{
    isOkay = _isOkay;
    if (owner)
        owner->addChild(*this);
}

void IfBlockOffsetInfo::adjustSize(int delta)
{
    if (isOkay)
        CompoundOffsetInfo::adjustSize(delta);
    else
        cachedSize += delta;
}

bool IfBlockOffsetInfo::isValid()
{
    if (!isOkay)
        return false;
    return CompoundOffsetInfo::isValid();
}



void IfBlockOffsetInfo::setValid(bool nowValid)
{
    if (isOkay != nowValid)
    {
        if (isOkay)
        {
            //Condition no longer true->shuffle contents up...
            unsigned offset = getOffset();
            int size = (int)getSize();
            owner->moveSection(offset, -size);
            adjustSize(-size);
            isOkay = false;
        }
        else
        {
            //unsigned offset = getOffset();
            //copy from offset to end of record to end of record.
            //create a default for this level (and possibly any child records).
            //child nodes need to notice that values have been created/destroyed
            //change the size of this element + parents
            //copy back onto end of record.
        }
    }
}

/*
Variable sized arrays.
    - Nodes know when their conditions change.  If possible change, structure is notified.
    - Checking and creation functions need 
      i) offset structure 
      ii) indexes to initialise (as an unsigned[])
      passed into them.  So they know what they are working on, and so not too inefficient.  Means sizes should
      always be consistent in the structure.
    - Doesn't work so well with grand-children, but will just about.

    Or
    - Nodes dynamically calculate condition from current settings.
    - CONCLUSION: Far to inefficient.......

    Or
    - Calling code always inform each item that changes in turn.
    - Would require loops to be generated in some circumstances.
    - Creation function only occurs one level at a time
    - Need explict code to , and always assumes false for children/
    - Problem: #elements could change by 1
    - CONCLUSION: In general too complicated/would get tied in knots.

Embedded child records
    1. Pointers are maintained for each of the parent/child records.
    2. Create/offset functions are passed pointers to each of the different levels of children.
    3. Would mean multiple "cursors" passed into denormalize/normalize functions.  Other activities could create internal cursors.
    4. Means you cannot access one child from another.
    5. Offset calculation becomes linear - no array complications.
    6. No array indexing since structure has been flattened
    7. No random modification because fields can always be updated in order.
*/


//---------------------------------------------------------------------------

AlienOffsetInfo::AlienOffsetInfo(OffsetInfoBase * _owner, OffsetInfoBase * _pred, unsigned _predDelta, unsigned _startSize) :
    OffsetInfoBase(_owner, _pred, _predDelta, _startSize)
{
    if (owner)
        owner->addChild(*this);
}



//############################################################################################################################################################
//############################################################################################################################################################
//############################################################################################################################################################
#if 0

class SizeChildArray : public PointerArray
{
public:
    SizeMonitor & item(unsigned idx) { return *(SizeMonitor *)item(idx); };
};


SizeSelector::SizeSelector(SizeMonitor & _monitor, unsigned maxDimensions) : monitor(_monitor)
{
    maxDimension s= _maxDimensions;
    curSelection = new unsigned[maxDimensions];
}

SizeSelector::~SizeSelector()
{
    delete [] curSelection;
}

unsigned SizeSelector::getSize()
{
    assertex(curDimension == maxDimensions);
    if (monitor->isValid(curSelection))
        return monitor.getSize(curSelection);
    return 0;
}

SizeSelector & SizeSelector::reset()
{
    curDimension = 0;
    return *this;
}

SizeSelector & SizeSelector::select(unsigned idx)
{
    assertex(curDimension < maxDimensions);
    curSelection[curDimension++] = idx;
    return *this;
}



//---------------------------------------------------------------------------


SizeMonitor::SizeMonitor(SizeMonitor * _owner, SizeMonitor * _pred, unsigned _predDelta) : curSelection(*this, _owner ? _owner->numDimensions() : 0)
{
    owner = _owner;
    pred = _pred;
    predDelta = _predDelta;
    maxDimensions = owner ? owner->numDimensions() : 0;
}

SizeSelector & SizeMonitor::get()
{
    return curSelection.reset();
}


void SizeMonitor::structureChanged()
{
    //MORE!
}


unsigned SizeMonitor::getSize(unsigned * curDimensions)
{
    return fixedSize;
}


//---------------------------------------------------------------------------

unsigned RecordMonitor::getSize(unsigned * curDimensions)
{
    unsigned size = fixedSize;
    ForEachItemIn(idx, *children)
        size += children->item(idx).getSize(curDimensions);
    return size;
}

//---------------------------------------------------------------------------

void SizeMonitor::adjustSize(unsigned * dimension, int delta)
{
    if (owner)
        owner->adjustSize(dimension, delta);
    cacheSize[getChildIndex(dimension)] += delta;
}

unsigned SizeMonitor::getChildIndex(unsigned * curDimensions)
{
    unsigned curIndex = 0;
    for (unsigend idx=0; idx < maxDimensions; idx++)
    {
        if (idx)
            curIndex *= maxDimensions[idx-1];
        curIndex += curDimensions[idx];
    }
    return curIndex;
}

void SizeMonitor::getOffset(unsigned * dimensions)
{
    if (pred)
        return pred->getOffset(dimensions) + pred->getOffset(dimensions) + fixedDelta;
    if (owner)
        return owner->getOffset(dimensions) + fixedDelta;
    return 0;
}


void SizeMonitor::getSize(unsigned * dimensions)
{
    return cachedSize[getChildIndex(dimensions)];
}


void SizeMonitor::dimensionChanged(unsigned dim, unsigned newDim)
{
    //check if need to expand max.
    /* Reallocate the cachedSizes, and copy the dimensions */
    //MORE!
}

//---------------------------------------------------------------------------


unsigned ArrayMonitor::calcSize(unsigned * curDimensions)
{
    unsigned size = 0;
    unsigned numDimensions = getDimension(curDimensions);
    if (children->ordinality())
    {
        SizeMonitor & child = children->item(0);

        SizeSelector iter(child, numDimensions+1);
        for (dim = 0; dim < numDimensions; dim++)
            size += iter.reset(numDimensions, curDimensions).select(dim).getSize() + fixedSize;
    }
    else
        size = fixedSize * numDimensions;
    return size;
}


void ArrayMonitor::getDimensions(unsigned * dimensions)
{
    return dimension[getChildIndex(dimensions)];
}



bool ArrayMonitor::isValid(unsigned * dimensions)
{
    if (!owner->isValid(dimensions))
        return false;
    if (getDimension(dimensions) == 0)
        return false;
    return true;
}

//---------------------------------------------------------------------------



void IfBlockMonitor::setShown(unsigned * dimensions, bool nowShown)
{
    if (isShown[dimensions] != nowShown)
    {
        if (isShown[dimensions])
        {
            isShown[dimensions] = false;
            owner->adjustSize(-getSize(dimensions))
        }
        else
        {
            isShown[dimensions] = false;
            owner->adjustSize(-getSize(dimensions))
        }
    }
}

#endif
