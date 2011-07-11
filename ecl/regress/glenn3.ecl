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

#option ('globalFold', false);
INTEGER1 i1     := 65;
INTEGER1 i1a    := 0x41;
INTEGER1 i1b    := 41x;
INTEGER1 i1c    := 0b01000001;
INTEGER1 i1d    := 01000001b;
 

OUTPUT(REJECTED(i1=i1a,i1=i1b,i1=i1c,i1=i1d));
OUTPUT(REJECTED(i1a=i1b,i1a=i1c,i1a=i1d));
OUTPUT(REJECTED(i1b=i1c,i1b=i1d));
OUTPUT(REJECTED(i1c=i1d));
 
OUTPUT((TRANSFER(i1,STRING1)));
OUTPUT((TRANSFER(i1a,STRING1)));
OUTPUT((TRANSFER(i1b,STRING1)));
OUTPUT((TRANSFER(i1c,STRING1)));
OUTPUT((TRANSFER(i1d,STRING1)));
