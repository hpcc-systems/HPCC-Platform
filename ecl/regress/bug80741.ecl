/*##############################################################################

    Copyright (C) <2010>  <LexisNexis Risk Data Management Inc.>

    All rights reserved. This program is NOT PRESENTLY free software: you can NOT redistribute it and/or modify
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

unicode u1 := u'abc';
data dataCast := (data)u1;
data dataTransfer := transfer(u1, data);

// expected 610062006300, but outputs 616263 // I'm guessing it's casting to STRING as an intermediate between UNICODE and DATA.
output(dataCast, named('cast_uni2data'));

// outputs 610062006300 as expected
output(dataTransfer, named('transfer_uni2data'));


data d1 := x'610062006300';
unicode uniCast := (unicode)d1;
unicode uniTransfer := (>unicode<)d1;

// expected abc, but looks like it outputs a<null>b<null>c<null>
output(uniCast, named('cast_data2uni'));

// expected abc, but looks like it outputs abc<null><null><null>
output(uniTransfer, named('transfer_data2uni'));

unicode uniTransfer2 := transfer(dataTransfer, unicode); // expected abc, but looks like it outputs abc<null><null><null><null><null><null><null><null><null>
output(uniTransfer2, named('transfer_uni2data2uni'));


unicode q1 := u'abc';
qstring qstringTransfer := transfer(u1, qstring);

output(qstringTransfer, named('transfer_uni2qstring'));
