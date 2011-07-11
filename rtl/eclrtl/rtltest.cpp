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
#include "jliball.hpp"
#include "eclrtl.hpp"

int main(void)
{
    unsigned ql;
    char * qp;
    rtlStrToQStrX(ql, qp, 14, "Gavin Halliday");

    unsigned ll;
    char * lp;
    rtlCreateQStrRangeLow(ll, lp, 20, 16, ql, qp);

    unsigned hl;
    char * hp;
    rtlCreateQStrRangeHigh(hl, hp, 20, 17, ql, qp);

    assertex(ll=20);
    assertex(hl=20);
    assertex(memcmp(lp, "\x9e\x1d\xa9\xb8\x0a\x21\xb2\xca\x64\x87\x90\x00\x00\x00\x00", 15)==0);
    assertex(memcmp(hp, "\x9e\x1d\xa9\xb8\x0a\x21\xb2\xca\x64\x87\x90\x00\x03\xFF\xFF", 15)==0);


    rtlFree(qp);
    rtlFree(lp);
    rtlFree(hp);
    return 1;
}
