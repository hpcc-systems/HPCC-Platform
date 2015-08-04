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

#option ('resourceSequential', true);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset('names',namesRecord,FLAT);


d1 := dataset([{'Hawthorn','Gavin',34}], namesRecord);
d2 := dataset([{'Hawthorn','Mia',34}], namesRecord);
d3 := dataset([{'Hawthorn','Abigail',2}], namesRecord);

boolean change := false : stored('change');

o1 := output(d1,,'names',overwrite);
o2 := output(d2,,'names',overwrite);
o2b := output(d3,,'names',overwrite);
o3 := if(change, o2);
o3b := if(false, o2b);
o4 := output(namesTable);

//sequential(o1, o3, o3b, o4);
sequential(o3b, o4);
