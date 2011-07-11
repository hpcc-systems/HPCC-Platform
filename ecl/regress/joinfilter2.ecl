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


rec1 := record
string10    value;
unsigned    did;
        end;

ds1 := dataset('ds', rec1, thor);
ds1f := ds1(value <> 0);
i1 := index(ds1, { ds1 }, 'i1');
i1f := index(ds1, { ds1 }, 'i1', filtered(value <> 0));

rec2 := record
qstring10   value;
unsigned    did;
        end;

ds2 := dataset('ds', rec2, thor);

i2 := index(ds2, { ds2 }, 'i2');
i2f := i1(value <> '');

searchRec := record
string10        str10;
qstring10       qstr10;
unsigned        u8;
set of string10 setstr10;
set of qstring10 setqstr10;
            end;

boolean alwaysMatch := false : stored('alwaysMatch');

searchDs := dataset('searchDs', searchRec, thor);

doJoins(i,extrajoin='\'\'',prekeyed='\'\'',postkeyed='\'\'') := macro
output(join(searchDs, i, #expand(prekeyed)((left.str10 = right.value #expand(extrajoin) #expand(postkeyed)))));
output(join(searchDs, i, #expand(prekeyed)((left.qstr10 = right.value #expand(extrajoin) #expand(postkeyed)))));
output(join(searchDs, i, #expand(prekeyed)((right.value in [left.str10, left.qstr10] #expand(extrajoin) #expand(postkeyed)))));
output(join(searchDs, i, #expand(prekeyed)((right.value in left.setstr10 #expand(extrajoin) #expand(postkeyed)))));
output(join(searchDs, i, #expand(prekeyed)((right.value in left.setqstr10 #expand(extrajoin) #expand(postkeyed)))));
endmacro;


doJoins(i1f);
doJoins(i1f, '', 'keyed');
doJoins(i1f, 'and right.value <> \'\'');
doJoins(i1f, 'and right.value <> \'\'', 'keyed');
/*
doJoins(i1, ') or (alwaysMatch');
doJoins(i1, ') or (alwaysMatch ', 'keyed');
doJoins(i1, 'and right.value <> \'\' ) or (alwaysMatch ');
doJoins(i1, 'and right.value <> \'\' ) or (alwaysMatch ', 'keyed');
doJoins(i2);
doJoins(i2, '', 'keyed');
doJoins(i2, 'and right.value <> \'\'');
doJoins(i2, 'and right.value <> \'\'', 'keyed');
doJoins(i2, ') or (alwaysMatch');
doJoins(i2, ') or (alwaysMatch ', 'keyed');
doJoins(i2, 'and right.value <> \'\' ) or (alwaysMatch ');
doJoins(i2, 'and right.value <> \'\' ) or (alwaysMatch ', 'keyed');
*/