/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2017 HPCC Systems®.

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

rec := RECORD
 STRING10 name;
 STRING1 sex;
END;

parentRec := RECORD
 string surname;
 DATASET(rec) cds;
END;

ds := DATASET([{'Smith',[{'John','M'},{'Heather','F'},{'Jim','M'}]},{'Jones',[{'Adam','M'},{'Gina','F'}]},{'Peters',[{'David','M'},{'Daniel','M'},{'Damien','M'}]},{'Jones',[{'Gloria','F'},{'Arthur','M'},{'Grace','F'}]},{'Smith',[{'James','M'},{'Hazel','F'},{'Helen','F'}]}], parentRec);

parentRec trans(parentRec l, parentRec r) := TRANSFORM
 sl := SORT(l.cds, sex);
 sr := SORT(r.cds, sex);
 mr := MERGEJOIN([sl, sr], LEFT.sex = RIGHT.sex, SORTED(sex));
 SELF.cds := IF(l.surname=r.surname, mr, sr);
 SELF := r;
END;

s := SORT(ds, surname);
i := ITERATE(s, trans(LEFT, RIGHT));
OUTPUT(i);
