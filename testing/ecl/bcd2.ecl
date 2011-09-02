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



decimal17_4 sum1 := 20;
unsigned count1 := 3;
output((decimal20_10)(sum1/count1));


decimal17_4 sum2 := 20 : stored('sum2');
unsigned count2 := 3 : stored('count2');
output((decimal20_10)(sum2/count2));

rec := { decimal17_4 sumx, unsigned countx };
ds := dataset([{20,3},{10,2},{10.0001,2}], rec);
output(nofold(ds), { sumx, countx, decimal20_10 average := sumx/countx, sumx between 10 and 10.00009, sumx between 10D and 10.00009D });


decimal17_4 value1 := 1.6667;
decimal17_4 value2 := 1.6667 : stored('value2');

output(round(value1));
output(roundup((real)value1));

output(round((real)value2));
output(roundup((real)value2));
