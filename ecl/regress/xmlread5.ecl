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


rec :=      RECORD
integer         duration{xpath('@duration')};
string          name{xpath('@name')};
            END;

timings := dataset('~file::127.0.0.1::temp::time.out', rec, XML('/All/Timings/Timing'));

output(topn(timings(name != 'EclServer: totalTime'), 30, -duration));
summary := table(timings, { integer total := sum(group, duration); name;}, name);
so := sort(summary, -total, name);
output(so);
