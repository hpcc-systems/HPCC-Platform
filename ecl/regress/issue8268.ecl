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

import std;
Tbl1RecDef := RECORD string10 fname; string10 lname; unsigned1 prange; string10 street; unsigned1 zips; unsigned1 age; string2 birth_state; string3 birth_month; unsigned1 one; unsigned8 id; unsigned8 __filepos {virtual(fileposition)}; END;

Tbl1DS := DATASET('~certification::full_test_distributed', Tbl1RecDef,FLAT);

Idx := INDEX(Tbl1DS, {lname, fname, prange, street, zips, age, birth_state, birth_month},{ __filepos },'~certification::full_test_distributed_index');

IdxDS := Idx(KEYED(  age > 10  ) and WILD(  lname  ) and WILD(  fname  ) and WILD(  prange  ) and WILD(  street  ) and WILD(  zips  ) and (age > 10 ));

SelectStruct := RECORD
maxOut := MAX( IdxDS( age > 10 ), IdxDS.age, KEYED );
string10 lname := IdxDS.lname;
string10 fname := IdxDS.fname;
END;

IdxDSTable := TABLE(IdxDS, SelectStruct );

OUTPUT(CHOOSEN(IdxDSTable,100),NAMED('JDBCSelectQueryResult'));