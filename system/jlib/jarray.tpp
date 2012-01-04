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


#ifndef CCLARRAY_TPP
#define CCLARRAY_TPP

/************************************************************************
 *                            Master BaseArrays                         *
 ************************************************************************/

template <class MEMBER, class PARAM>
void BaseArrayOf<MEMBER, PARAM>::append(PARAM add)
{
   SELF::_space();
   MEMBER * head= (MEMBER *)SELF::_head;
   Array__Assign(head[SELF::used-1], add);
}

template <class MEMBER, class PARAM>
bool BaseArrayOf<MEMBER, PARAM>::appendUniq(PARAM add)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   for (aindex_t pos = SELF::used; pos;) 
      if (Array__Equal(head[--pos], add))
         return false;
   SELF::_space();
   head= (MEMBER *)SELF::_head;
   Array__Assign(head[SELF::used-1], add);
   return true;
}

template <class MEMBER, class PARAM>
void BaseArrayOf<MEMBER, PARAM>::add(PARAM it, aindex_t pos)
{
   aindex_t valid_above = SELF::used - pos;
   SELF::_space();

   MEMBER * head= (MEMBER *)SELF::_head;
   SELF::_move(pos + 1, pos, valid_above);
   Array__Assign(head[pos], it);
}

template <class MEMBER, class PARAM>
aindex_t BaseArrayOf<MEMBER, PARAM>::bAdd(MEMBER & newItem, CompareFunc cf, bool & isNew)
{
   MEMBER * match;

   SELF::_space();

   MEMBER * head= (MEMBER *)SELF::_head;
   match = (MEMBER *) this->_doBAdd(&newItem, sizeof(MEMBER), (StdCompare)cf, isNew);
   if (!isNew)
      SELF::used--;
   return (aindex_t)(match - head);
}


template <class MEMBER, class PARAM>
aindex_t BaseArrayOf<MEMBER, PARAM>::bSearch(const MEMBER & key, CompareFunc cf) const
{
   MEMBER * head= (MEMBER *)SELF::_head;
   MEMBER * match;

   match = (MEMBER *) this->_doBSearch(&key, sizeof(MEMBER), (StdCompare)cf);

   if (match)
      return (aindex_t)(match - (MEMBER *)head);
   return NotFound;
}

template <class MEMBER, class PARAM>
MEMBER *BaseArrayOf<MEMBER, PARAM>::getArray(aindex_t pos) const
{
   MEMBER * head= (MEMBER *)SELF::_head;
   assertex(pos <= SELF::used);
   return &head[pos];
}

template <class MEMBER, class PARAM>
aindex_t BaseArrayOf<MEMBER, PARAM>::find(PARAM sought) const
{
   MEMBER * head= (MEMBER *)SELF::_head;
   for (aindex_t pos = 0; pos < SELF::used; ++pos)
      if (Array__Equal(head[pos], sought))
         return pos;
   return NotFound;
}

template <class MEMBER, class PARAM>
void BaseArrayOf<MEMBER, PARAM>::swap(aindex_t pos1, aindex_t pos2)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   MEMBER temp = head[pos1];
   head[pos1] = head[pos2];
   head[pos2] = temp;
}


template <class MEMBER, class PARAM>
void BaseArrayOf<MEMBER, PARAM>::sort(CompareFunc cf)
{
   SELF::_doSort(sizeof(MEMBER), (StdCompare)cf);
}

/************************************************************************
 *                            Master CopyArrays                         *
 ************************************************************************/

#ifdef __clang__
template <class MEMBER, class PARAM>
PARAM CopyArrayOf<MEMBER, PARAM>::item(aindex_t pos) const
{
   assertex(SELF::isItem(pos)); return Array__Member2Param(((MEMBER *)AllocatorOf<sizeof(MEMBER)>::_head)[pos]);
}
#endif

template <class MEMBER, class PARAM>
void CopyArrayOf<MEMBER, PARAM>::replace(PARAM it, aindex_t pos)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   Array__Assign(head[pos], it);
}

template <class MEMBER, class PARAM>
void CopyArrayOf<MEMBER, PARAM>::remove(aindex_t pos)
{
   assertex(pos < SELF::used);  
   SELF::used --;
   SELF::_move( pos, pos + 1, ( SELF::used - pos ) );
}

template <class MEMBER, class PARAM>
void CopyArrayOf<MEMBER, PARAM>::removen(aindex_t pos, aindex_t num)
{
   assertex(pos + num <= SELF::used);  
   SELF::used -= num;
   SELF::_move( pos, pos + num, ( SELF::used - pos ) );
}

template <class MEMBER, class PARAM>
PARAM CopyArrayOf<MEMBER, PARAM>::pop(void)
{
   PARAM ret = SELF::tos();
   assertex(SELF::used);
   --SELF::used;
   return ret;
}

template <class MEMBER, class PARAM>
void CopyArrayOf<MEMBER, PARAM>::popAll(void)
{
   SELF::used = 0;
}

template <class MEMBER, class PARAM>
void CopyArrayOf<MEMBER, PARAM>::popn(aindex_t n)
{
   assertex(SELF::used>=n);
   SELF::used -= n;
}

template <class MEMBER, class PARAM>
CopyArrayOf<MEMBER, PARAM>::~CopyArrayOf()
{
   SELF::kill();
}

template <class MEMBER, class PARAM>
PARAM CopyArrayOf<MEMBER, PARAM>::tos() const
{
   assertex(SELF::used);
   return item(SELF::used-1);
}

template <class MEMBER, class PARAM>
PARAM CopyArrayOf<MEMBER, PARAM>::tos(aindex_t n) const
{
   assertex(SELF::used > n);
   return item(SELF::used-n-1);
}

template <class MEMBER, class PARAM>
void CopyArrayOf<MEMBER, PARAM>::trunc(aindex_t limit)
{
   if (limit < SELF::used)
      SELF::used = limit;
}

template <class MEMBER, class PARAM>
bool CopyArrayOf<MEMBER, PARAM>::zap(PARAM sought)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   for (aindex_t pos= 0; pos < SELF::used; ++pos)
      if (Array__Equal(head[pos], sought))
      {
         remove(pos);
         return true;
      }
   return false;
}

/************************************************************************
 *                            Master Lists                              *
 ************************************************************************/

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::kill(bool nodestruct)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   aindex_t count = SELF::used;

   SELF::used = 0;
   if (!nodestruct)
   {
      for (aindex_t i=0; i<count; i++)
         Array__Destroy(head[i]);
   }
   PARENT::kill();
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::replace(PARAM it, aindex_t pos, bool nodestruct)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   if (!nodestruct) Array__Destroy(head[pos]);
   Array__Assign(head[pos], it);
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::remove(aindex_t pos, bool nodestruct)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   assertex(pos < SELF::used);  
   SELF::used --;
   if (!nodestruct) Array__Destroy(head[pos]);
   SELF::_move( pos, pos + 1, ( SELF::used - pos ) );
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::removen(aindex_t pos, aindex_t num, bool nodestruct)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   assertex(pos + num <= SELF::used);  
   SELF::used -= num;
   if (!nodestruct)
   {
     unsigned idx = 0;
     for (;idx < num; idx++)
       Array__Destroy(head[pos+idx]);
   }
   SELF::_move( pos, pos + num, ( SELF::used - pos ) );
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::pop(bool nodestruct)
{
   assertex(SELF::used);
   MEMBER * head= (MEMBER *)SELF::_head;
   --SELF::used;
   if (!nodestruct) Array__Destroy(head[SELF::used]);
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::popn(aindex_t n,bool nodestruct)
{
   assertex(SELF::used>=n);
   MEMBER * head= (MEMBER *)SELF::_head;
   while (n--)
   {
      --SELF::used;
      if (!nodestruct) Array__Destroy(head[SELF::used]);
   }
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::popAll(bool nodestruct)
{
   if (!nodestruct)
   {
      MEMBER * head= (MEMBER *)SELF::_head;
      while (SELF::used)
         Array__Destroy(head[--SELF::used]);
   }
   else
      SELF::used = 0;
}


template <class MEMBER, class PARAM>
OwningArrayOf<MEMBER, PARAM>::~OwningArrayOf()
{
   SELF::kill();
}

template <class MEMBER, class PARAM>
void OwningArrayOf<MEMBER, PARAM>::trunc(aindex_t limit, bool nodel)
{
   while (limit < SELF::used)
      pop(nodel);
}


template <class MEMBER, class PARAM>
bool OwningArrayOf<MEMBER, PARAM>::zap(PARAM sought, bool nodel)
{
   MEMBER * head= (MEMBER *)SELF::_head;
   for (aindex_t pos= 0; pos < SELF::used; ++pos)
      if (Array__Equal(head[pos], sought))
      {
         remove(pos, nodel);
         return true;
      }
   return false;
}

/************************************************************************
 *                            Master Ref Array                          *
 ************************************************************************/

#ifdef __clang__
template <class MEMBER, class PARAM>
PARAM ArrayOf<MEMBER, PARAM>::item(aindex_t pos) const
{
   assertex(SELF::isItem(pos)); return Array__Member2Param(((MEMBER *)AllocatorOf<sizeof(MEMBER)>::_head)[pos]);
}
#endif

template <class MEMBER, class PARAM>
PARAM ArrayOf<MEMBER, PARAM>::popGet()
{
   PARAM ret = ArrayOf<MEMBER, PARAM>::tos();
   assertex(SELF::used);
   --SELF::used;
   return ret;
}

template <class MEMBER, class PARAM>
PARAM ArrayOf<MEMBER, PARAM>::tos() const
{
   assertex(SELF::used);
   return item(SELF::used-1);
}

template <class MEMBER, class PARAM>
PARAM ArrayOf<MEMBER, PARAM>::tos(aindex_t n) const
{
   assertex(SELF::used > n);
   return item(SELF::used-n-1);
}

/************************************************************************
 *                            Master Ptr Array                          *
 ************************************************************************/

template <class MEMBER, class PARAM>
PARAM PtrArrayOf<MEMBER, PARAM>::popGet()
{
   PARAM ret = PtrArrayOf<MEMBER, PARAM>::tos();
   assertex(SELF::used);
   --SELF::used;
   return ret;
}

template <class MEMBER, class PARAM>
PARAM PtrArrayOf<MEMBER, PARAM>::tos() const
{
   assertex(SELF::used);
   return item(SELF::used-1);
}

template <class MEMBER, class PARAM>
PARAM PtrArrayOf<MEMBER, PARAM>::tos(aindex_t n) const
{
   assertex(SELF::used > n);
   return item(SELF::used-n-1);
}

#endif


