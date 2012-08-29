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
