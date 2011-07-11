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




//This function is included twice in order to common up the code....

/*
void * binary_add(const void *newitem, const void *base,
             size_t nmemb, 
             WIDTH_PROTOTYPE
             COMPARE_PROTOTYPE
             bool * ItemAdded)
{
*/
/*
 * Perform a binary search and add.  The entry is only added if it
 * does not already exist.
 * A pointer to the new entry is returned.

 */
   size_t      right;
   size_t      middle;
   size_t      left = 0;
   int         result;
   char *      CurEntry;

#ifndef ALWAYS_ADD
   *ItemAdded = false;
#endif

   if (nmemb == 0)
   {
      middle = 0;
      goto AddEntry;
   }

   right = nmemb-1;

   /*
    * Loop until we've narrowed down the search range to one or two
    * items.  This ensures that middle != 0, preventing 'right' from wrapping.
    */
   while (right - left > 1)
   {
      /* Calculate the middle entry in the array - NOT the middle offset */
      middle = (right + left) >> 1;

      CurEntry = (char *)base + middle * width;
      result = COMPARE(newitem, CurEntry);

      if (result < 0)
         right = middle - 1;
      else if (result > 0)
         left = middle + 1;
      else
         goto Done;
   }

   middle = left;
   /*
    * The range has been narrowed down to 1 or 2 items.
    * Perform an optimal check on these last two elements.
    */
   result = COMPARE(newitem, (char *)base + middle * width);
   if (result == 0)
      goto Done;

   if (result > 0)
   {
      ++middle;

      if (right == left + 1)
      {
         result = COMPARE(newitem, (char *)base + middle * width);
         if (result == 0)
            goto Done;
         if (result > 0)
            ++middle;
      }
   }

AddEntry:
#ifndef NEVER_ADD
#ifdef ALWAYS_ADD
Done:
#endif
   CurEntry = (char *)base + middle * width;
#ifdef SEEK_LAST_MATCH
   while((middle < nmemb) && (COMPARE(newitem, CurEntry) == 0))
   {
      ++middle;
      CurEntry += width;
   }
#endif
   memmove(CurEntry + width, CurEntry, (nmemb - middle) * width);
   INSERT(CurEntry, newitem);
#ifndef ALWAYS_ADD
   *ItemAdded = true;
Done:
#endif
#else
   *ItemAdded = true;
Done:
#endif
   return (char *)base + middle * width;
/*
}
*/
