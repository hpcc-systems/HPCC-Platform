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

import lib_stringlib;
extractXStringLength(data x, unsigned len) := transfer(((data4)(x[1..len])), unsigned4);

//A pretty weird type example - a length prefixed string, where the number of bytes used for the length is configurable...
xstring(unsigned len) := type
    export integer physicallength(data x) := extractXStringLength(x, len)+len;
    export string load(data x) := (string)x[(len+1)..extractXStringLength(x, len)+len];
    export data store(string x) := transfer(length(x), data4)[1..len]+(data)x;
end;

reverseString4 := TYPE
   shared STRING4 REV(STRING4 S) := S[4] + S[3] + S[2] +S[1];
   EXPORT STRING4 LOAD(STRING4 S) := REV(S);
   EXPORT STRING4 STORE(STRING4 S) := REV(S);
END;

 dstring(string del) := TYPE
    export integer physicallength(string s) := StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
    export string load(string s) := s[1..StringLib.StringUnboundedUnsafeFind(s,del)-1];
    export string store(string s) := s+del; // Untested (vlength output generally broken)
END;


pstring := xstring(1);
ppstring := xstring(2);
pppstring := xstring(3);
nameString := string20;

namesRecord :=
            RECORD
pstring         surname;
nameString      forename;
pppString       addr;
reverseString4  extra;
dstring('!')    extra2;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin','Slapdash Lane', 'ABCD', ''},
        {'Hawthorn','Mia','Slapdash Lane', '1234', 'groan'},
        {'Smithe','Pru','Ashley Road', 'PLEH', 'grown' },
        {'X','Z','Mars','!EM ', 'help'}], namesRecord);

saved1 := namesTable : independent(many);

saved2 := saved1 : independent(few);

saved3 := saved2 : independent(many);

saved4 := TABLE(saved3, { saved3, unsigned forceTransform := 0; }) : independent(few);

saved5 := TABLE(saved4, { saved4 } - [forceTransform]);

output(saved5);
