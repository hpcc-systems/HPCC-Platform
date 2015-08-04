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

d := dataset('s.csv', { unsigned4 qid, string20 fieldname, string20 condition, string1000 value}, CSV);

d1 := dedup(group(d, qid), fieldname);

summary := record
unsigned4 qid;
unsigned4 fields;
end;

d1 t1(d1 l, d1 r) := transform
SELF.qid := r.qid;
SELF.condition := '';
SELF.fieldname := '';
SELF.value := IF(l.fieldname!='',TRIM(l.value)+','+r.fieldname,r.fieldname);
END;

d2 := rollup(d1, qid, t1(LEFT, RIGHT));

//output(choosen(d2, 1),,'fff', OVERWRITE);

count(choosen(d2, 1));
