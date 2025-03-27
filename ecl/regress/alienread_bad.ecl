/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

//check that virtual fields in the middle of a record with alein datatypes reports an error.
#option ('targetClusterType', 'hthor');
#option ('genericDiskReadWrites', true);

import lib_stringlib;
prefix := 'regress::'+ __TARGET_PLATFORM__ + '::';

extractXStringLength(data x, unsigned len) := transfer(((data4)(x[1..len])), unsigned4);

//A pretty weird type example - a length prefixed string, where the number of bytes used for the length is configurable...
xstring(unsigned len) := type
    export integer physicallength(data x) := extractXStringLength(x, len)+len;
    export string load(data x) := (string)x[(len+1)..extractXStringLength(x, len)+len];
    export data store(string x) := transfer(length(x), data4)[1..len]+(data)x;
end;

dstring(string del) := TYPE
    export integer physicallength(string s) := StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
    export string load(string s) := s[1..StringLib.StringUnboundedUnsafeFind(s,del)-1];
    export string store(string s) := s+del; // Untested (vlength output generally broken)
END;


alienString := xstring(4);
alienVarString := dstring('\000');

alienRecordEx := RECORD
    alienString         surname;
    alienString         forename;
    alienVarString      addr;
    string4             extra;
    unsigned        fpos{virtual(fileposition)};
    alienVarString      extra2;
    string          filename{virtual(logicalfilename)};
    unsigned        lfpos{virtual(localfileposition)};
END;

filenameRaw := prefix+'alientest_raw';
filenameAlien := prefix+'alientest_alien';

output(DATASET(filenameRaw, alienRecordEx, THOR, HINT(layoutTranslation('alwaysEcl'))),,NAMED('RawAlien'));
