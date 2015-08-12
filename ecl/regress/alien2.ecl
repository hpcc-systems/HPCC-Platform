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

export extractXStringLength(data x, unsigned len) := transfer(((data4)(x[1..len])), unsigned4);

//A pretty weird type example - a length prefixed string, where the number of bytes used for the length is configurable...
export xstring(unsigned len) := type
    export integer physicallength(data x) := extractXStringLength(x, len)+len;
    export string load(data x) := (string)x[(len+1)..extractXStringLength(x, len)+len];
    export data store(string x) := transfer(length(x), data4)[1..len]+(data)x;
end;

pstring := xstring(1);
ppstring := xstring(2);
pppstring := xstring(3);
nameString := string20;

namesRecord :=
            RECORD
pstring         surname;
nameString      forename;
pppString       addr;
            END;

namesTable := dataset('x',namesRecord,FLAT);

namesTable2 := dataset([
        {'Hawthorn','Gavin','Slapdash Lane'},
        {'Hawthorn','Mia','Slapdash Lane'},
        {'Smithe','Pru','Ashley Road'},
        {'X','Z','Mars'}], namesRecord);

output(namesTable2,,'x');
