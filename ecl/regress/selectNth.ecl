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

#option ('targetClusterType','thor');
#option ('pickBestEngine', 0);

myrecord := record
 real diff;
 integer1 reason;
  end;

rms5008 := 10;
rms5009 := 11;
rms5010 := 12;
rms5011 := 13;

btable :=
dataset([{rms5008,72},{rms5009,7},{rms5010,30},{rms5011,31}],myrecord);

RCodes := sort(btable,-btable.diff);

RCode1 := RCodes[1].reason;
RCode2 := RCodes[2].reason;

RCode1;
RCode2;

// use evaluate
RCode1x := evaluate(RCodes[1],RCodes.reason);
RCode2x := evaluate(RCodes[2],RCodes.reason);
RCode1x;
Rcode2x;

RCodes[10].reason;

