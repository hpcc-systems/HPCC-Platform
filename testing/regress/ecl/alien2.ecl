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
import $.setup;
prefix := setup.Files(false, false).FilePrefix + __TARGET_PLATFORM__ + '::';

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

namesRecord := RECORD
    string          surname;
    string          forename;
    varstring       addr;
    string4         extra;
    varstring       extra2;
END;

namesRecordEx := RECORD(namesRecord)
    unsigned        fpos{virtual(fileposition)};
    string          filename{virtual(logicalfilename)};
    unsigned        lfpos{virtual(localfileposition)};
END;

reducedNamesRecordEx := namesRecordEx - [addr, extra, lfpos];

alienRecord := RECORD
    alienString         surname;
    alienString         forename;
    alienVarString      addr;
    string4             extra;
    alienVarString      extra2;
END;

alienRecordEx := RECORD(alienRecord)
    unsigned        fpos{virtual(fileposition)};
    string          filename{virtual(logicalfilename)};
    unsigned        lfpos{virtual(localfileposition)};
END;


filenameRaw := prefix+'alientest_raw';
filenameAlien := prefix+'alientest_alien';

namesTable := dataset([
        {'Hawthorn','Gavin','Slapdash Lane', 'ABCD', ''},
        {'Hawthorn','Mia','Slapdash Lane', '1234', 'groan'},
        {'Smithe','Pru','Ashley Road', 'PLEH', 'grown' },
        {'X','Z','Mars','!EM ', 'help'}], namesRecord);

p := PROJECT(namesTable, TRANSFORM(alienRecord, SELF := LEFT));

sequential(
    //Output the files two ways - one using alien data types, and one using the equiavlent built in types.
    output(namesTable,,filenameRaw, thor, overwrite);
    output(p,,filenameAlien, thor, overwrite);

    //Now read the files back in different ways

    output(DATASET(filenameRaw, namesRecordEx, THOR),,NAMED('RawRaw'));
    output(DATASET(filenameRaw, alienRecordEx, THOR, __OPTION__(LEGACY),HINT(layoutTranslation('alwaysEcl'))),,NAMED('RawAlien'));
    output(DATASET(filenameAlien, namesRecordEx, THOR,HINT(layoutTranslation('alwaysEcl'))),,NAMED('AlienRaw'));
    output(DATASET(filenameAlien, alienRecordEx, THOR, __OPTION__(LEGACY)),,NAMED('AlienAlien'));

    //Read the files in and also project them - to test mixing projection and untranslatable file formats works correctly
    output(PROJECT(DATASET(filenameRaw, namesRecordEx, THOR), transform(reducedNamesRecordEx, SELF := LEFT)),,NAMED('ProjectRawRaw'));
    output(PROJECT(DATASET(filenameRaw, alienRecordEx, THOR, __OPTION__(LEGACY),HINT(layoutTranslation('alwaysEcl'))), transform(reducedNamesRecordEx, SELF := LEFT)),,NAMED('ProjectRawAlien'));
    output(PROJECT(DATASET(filenameAlien, namesRecordEx, THOR,HINT(layoutTranslation('alwaysEcl'))), transform(reducedNamesRecordEx, SELF := LEFT)),,NAMED('ProjectAlienRaw'));
    output(PROJECT(DATASET(filenameAlien, alienRecordEx, THOR, __OPTION__(LEGACY)), transform(reducedNamesRecordEx, SELF := LEFT)),,NAMED('ProjectAlienAlien'))
);
