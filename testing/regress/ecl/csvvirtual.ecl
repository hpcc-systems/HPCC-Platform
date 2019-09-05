/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2019 HPCC Systems®.

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

//xversion format='ASCII'   // has different filepositions
//version format='UNICODE'
//xversion format='ASCII',optRemoteRead=true
//xversion format='UNICODE',optRemoteRead=true

//noroxie      - see HPCC-22629

import ^ as root;
csvFormat := #IFDEFINED(root.format, 'UNICODE') + ',';
optRemoteRead := #IFDEFINED(root.optRemoteRead, false);

import $.setup;
prefix := setup.Files(false, false).QueryFilePrefix;

// Roxie needs this to resolve files at run time
#option ('allowVariableRoxieFilenames', 1);
#option('forceRemoteRead', optRemoteRead);

VarString EmptyString := '' : STORED('dummy');

rec3 := RECORD
  string f1;
  string f2;
  string f3;
  unsigned8 filepos{virtual(fileposition)};
  unsigned8 filepos2{virtual(localfileposition)};
END;

rec4 := RECORD//(rec3)  // inheritance currently doesn't work, need to investigate why not
  string f1;
  string f2;
  string f3;
  unsigned8 filepos{virtual(fileposition)};
  unsigned8 filepos2{virtual(localfileposition)};
  string filename{virtual(logicalfilename)};
END;

textLines := DATASET([
    '!abc!,dêf,gêll',
    '!abc,"dêf!,hêllo"',
    '"one " ,"two "," thrêê "'
    ], { string line });

filename := prefix + 'csv-options'+EmptyString;

inDs := DATASET('{' + filename + ',' + filename + '}', rec3, CSV(#EXPAND(csvFormat) MAXSIZE(9999)));
inDs2 := DATASET('{' + filename + ',' + filename + '}', rec4, CSV(#EXPAND(csvFormat) MAXSIZE(9999)));

sequential(
    OUTPUT(textLines, ,filename, OVERWRITE, CSV(#EXPAND(csvFormat) MAXSIZE(9999)));
    output('Quote');
    output(inDs);
    output(inDs, { f1 });
    output(inDs, { f1, filepos, filepos2 });
    output(inDs, { f1, filepos2, filepos });
    output(inDs, { f1, DATA8 castlocal := (>DATA8<)(big_endian unsigned8)filepos2, filepos });
    output(inDs, { filepos2, f3, filepos });
    output(inDs, { f3, filepos2, f2, f1, filepos });
    output(inDs, { f1, filepos, biaslocal := (unsigned8)filepos2-(unsigned8)0x8000000000000000});
    output(inDs2, { f1, filepos, biaslocal := (unsigned8)filepos2-(unsigned8)0x8000000000000000});
    //output(inDs2, { f1, filepos, biaslocal := (unsigned8)filepos2-(unsigned8)0x8000000000000000});
    //output(inDs2, { f1, filepos, filepos2, filename[length(filename)-13..] });  Currently generates a compile error... JIRA #xxxx
);