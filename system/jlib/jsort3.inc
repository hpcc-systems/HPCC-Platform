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


// skeleton for heapsort - define macros for ELEM and LT to use

//void heapsort(ELEM *elems, unsigned n)
{
    if (n==0)
        return;
    ELEM tmp;
    unsigned k;
    for (k=0;k<n;k++) {
        tmp = elems[k];
        unsigned i = k;
        while (i>0) {
            unsigned j = (i-1)/2;
            if (LT(elems[j],tmp)) {
                elems[i] = elems[j];
                i = j;
            }
            else
                break;
        }
        elems[i] = tmp;
    }
    for (k=n-1; k>0; k--) {
        tmp = elems[k];
        elems[k] = elems[0]; 
        unsigned j=0;
        loop {
            unsigned p=2*(j+1);
            unsigned i;
            if (p>k)
                break;
            if ((p!=k)&&LT(elems[p-1],elems[p]))
                i = p;
            else
                i = p-1;
            if (LT(tmp,elems[i]))
                elems[j] = elems[i];
            else
                break;
            j = i;
        }
        elems[j] = tmp;
    }
}
