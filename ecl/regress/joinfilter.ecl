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