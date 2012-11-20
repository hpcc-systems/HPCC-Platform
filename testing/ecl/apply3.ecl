/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

r := { unsigned id; };
o := { string10 forename; unsigned id; };

namesRecord :=  RECORD
 string20       surname;
 string10       forename;
 dataset(r)     ids;
END;

namesTable := dataset([
        {'Hawthorn','Gavin',[{1},{2},{3}]},
        {'Hawthorn','Mia',[{4},{5},{8}]},
        {'Smithe','Pru',[{11}]},
        {'X','Z',[{10}]}], namesRecord);

f := namesTable.ids(id & 1 = 1);

apply(namesTable,
    output(PROJECT(f, TRANSFORM(o, SELF.id := LEFT.id; SELF.forename := namesTable.forename)),named('Result'),EXTEND),
    output(PROJECT(f, TRANSFORM(o, SELF.id := LEFT.id * 3; SELF.forename := namesTable.forename)),named('Result'),EXTEND)
    );

