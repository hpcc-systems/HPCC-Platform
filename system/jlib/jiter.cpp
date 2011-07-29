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


#define no_errors
#include "jiter.hpp"
#include "jiter.ipp"

//==============================================================================================================

CArrayIteratorBase::CArrayIteratorBase(const Array &_values, aindex_t _start, IInterface *_owner) : owner(_owner), values (_values), start(_start)
{
    current = start;
    ::Link(_owner);
}

CArrayIteratorBase::~CArrayIteratorBase()
{
    ::Release(owner);
}

bool CArrayIteratorBase::first()
{
    current = start;
    return values.isItem(current);
}

bool CArrayIteratorBase::next()
{
    current++;
    return values.isItem(current);
}

bool CArrayIteratorBase::isValid()
{
    return values.isItem(current);
}

IInterface & CArrayIteratorBase::_query()
{
    return values.item(current);
}


COwnedArrayIterator::COwnedArrayIterator(Array *_values, aindex_t _start) 
    : CArrayIterator(*_values, _start)
{
}

COwnedArrayIterator::~COwnedArrayIterator()
{
    delete &values;
}


//==============================================================================================================


