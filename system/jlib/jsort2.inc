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


//void qsortvec(void **a, size_t n, size_t es)
{
#ifdef PARANOID
    void **starta = a;
    size_t startn = n;
#endif

    VECTOR pa;
    VECTOR pb;
    VECTOR pc; 
    VECTOR pd;
    VECTOR pl;
    VECTOR pm;
    VECTOR pn;
    size_t d, swap_cnt;
    int r;

reloop:
    swap_cnt = 0;
    if (n < 7) {
        for (pm = a + 1; pm < a + n; pm++)
            for (pl = pm; pl > a && CMP(pl-1, pl) > 0; pl--)
                SWAP(pl, pl-1);
        return;
    }
    pm = a + (n / 2);
    if (n > 7) {
        pl = a;
        pn = a + (n - 1) ;
        if (n > 40) {
            d = (n / 8);
            pl = MED3(pl, pl + d, pl + 2 * d);
            pm = MED3(pm - d, pm, pm + d);
            pn = MED3(pn - 2 * d, pn - d, pn);
        }
        pm = MED3(pl, pm, pn);
    }
    SWAP(a, pm);
    pa = pb = a + 1;

    pc = pd = a + (n - 1);
    for (;;) {
        while (pb <= pc && (r = CMP(pb, a)) <= 0) {
            if (r == 0) {
                swap_cnt = 1;
                SWAP(pa, pb);
                pa++;
            }
            pb++;
        }
        while (pb <= pc && (r = CMP(pc, a)) >= 0) {
            if (r == 0) {
                swap_cnt = 1;
                SWAP(pc, pd);
                pd--;
            }
            pc--;
        }
        if (pb > pc)
            break;
        SWAP(pb, pc);
        swap_cnt = 1;
        pb++;
        pc--;
    }
    if ((swap_cnt == 0)&&(n<1000)) {  /* Switch to insertion sort */
        for (pm = a + 1; pm < a + n; pm++)
            for (pl = pm; pl > a && CMP(pl - 1, pl) > 0; pl --)
                SWAP(pl, pl-1);
        return;
    }

    pn = a + n;
    r = MIN(pa - a, pb - pa);
    VECTOR v1 = a;
    VECTOR v2 = pb-r;
    while (r) {
        SWAP(v1,v2); v1++; v2++; r--;
    };
    r = MIN(pd - pc, pn - pd - 1);
    v1 = pb;
    v2 = pn-r;
    while (r) {
        SWAP(v1,v2); v1++; v2++; r--;
    };
    // iterates longest sequence to save stack space
    int r1 = pb-pa;
    int r2 = pd-pc;
    if (r1<r2) {
        if (r1>1)
            RECURSE(a, r1);
        a = pn - r2;
        n = r2;
    }
    else {      
        if (r2>1)
            RECURSE(pn-r2, r2);
        n = r1;
    }
#ifndef PARANOID
    // if (n>1)         // not needed as we know probably is already
        goto reloop;
#else
    RECURSE(a, n);
    a = starta;
    n = startn;
    for (pa = a + 1; pa < a + n; pa++)
        if (CMP(pa-1, pa) > 0) {
            PrintLog("***qsortvec failed sorting");
            assertex("qsortvec failed");
        }
#endif
}
