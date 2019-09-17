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

//version format='ASCII'
//version format='UNICODE'
//version format='ASCII',optRemoteRead=true
//version format='UNICODE',optRemoteRead=true
//xxversion format='EBCDIC'     output doesn't seem to be supported currently, and not sure input is ever used

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
END;

rec5 := RECORD(rec3)
  string f4;
  string f5;
END;

textLines := DATASET([
    {'!abc!','dêf','gêll\'s'},
    {'!abc','"dêf!','hêllo"'},
    {'"one " ','"two "','" thrêê "'}
    ], rec3);

OUTPUT(textLines, ,prefix + 'csv-options'+EmptyString, OVERWRITE, CSV(#EXPAND(csvFormat) QUOTE('\''),SEPARATOR('$$'),TERMINATOR(';'),MAXSIZE(9999)));


generateOutput3(options) := MACRO
    OUTPUT(DATASET(prefix + 'csv-options'+EmptyString, rec3, CSV(#EXPAND(csvFormat) #EXPAND(options) MAXSIZE(9999))))
ENDMACRO;

generateOutput5(options) := MACRO
    OUTPUT(DATASET(prefix + 'csv-options'+EmptyString, rec5, CSV(#EXPAND(csvFormat) #EXPAND(options) MAXSIZE(9999))))
ENDMACRO;

output('Quote');
generateOutput3('');
generateOutput3('QUOTE(\'\'),');
generateOutput3('QUOTE(\'!\'),');
generateOutput3('QUOTE(\'"\'),');
generateOutput3('QUOTE(\'\'),NOTRIM,');
generateOutput3('QUOTE(\'"\'),NOTRIM,');

output('Separator');
generateOutput3('SEPARATOR(\'!\'),');
generateOutput5('SEPARATOR(\'ê\'),');
generateOutput5('SEPARATOR(\'ê\'),');  //The following currently ignores the second item generateOutput5('SEPARATOR(\'ê\'),SEPARATOR(\',\'),');
generateOutput5('SEPARATOR([\'ê\',\',\']),');

output('Terminator');
generateOutput3('QUOTE(\'\'),TERMINATOR(\'!\'),');
generateOutput3('QUOTE(\'\'),TERMINATOR(\'\\n\'),');            //You shouldn't really need to use \\n, but too likely to break things

output('Heading');
generateOutput5('SEPARATOR(\'ê\'),HEADING(0),');
generateOutput5('SEPARATOR(\'ê\'),HEADING(1),');
generateOutput5('SEPARATOR(\'ê\'),HEADING(2),');

output('Escape');
generateOutput5('ESCAPE(\'!\'),');
