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

// Roxie needs this to resolve files at run time
#option ('allowVariableRoxieFilenames', 1);
VarString EmptyString := '' : STORED('dummy');

rec := RECORD
  string foo;
  integer id;
  string bar;
END;

// Default is no escape
orig := DATASET([{'this is an \\\'escaped\\\' string', 10, 'while this is not'}], rec);
OUTPUT(orig, ,'regress::csv-orig'+EmptyString, OVERWRITE, CSV);
escaped := DATASET('regress::csv-orig'+EmptyString, rec, CSV);
OUTPUT(escaped);

// Standard escape
orig2 := DATASET([{'this is an \\\'escaped\\\' string', 10, 'while this is not'}], rec);
OUTPUT(orig2, ,'regress::csv-escaped'+EmptyString, OVERWRITE, CSV);
escaped2 := DATASET('regress::csv-escaped'+EmptyString, rec, CSV(ESCAPE('\\')));
OUTPUT(escaped2);

// Multi-char escape
orig3 := DATASET([{'this is an -=-\'escaped-=-\' string', 10, 'while this is not'}], rec);
OUTPUT(orig3, ,'regress::csv-escaped-multi'+EmptyString, OVERWRITE, CSV);
escaped3 := DATASET('regress::csv-escaped-multi'+EmptyString, rec, CSV(ESCAPE('-=-')));
OUTPUT(escaped3);

// Escape the escape
orig4 := DATASET([{'escape the \\\\ escape', 10, 'escape at the end \\\\'}], rec);
OUTPUT(orig4, ,'regress::csv-escaped-escape'+EmptyString, OVERWRITE, CSV);
escaped4 := DATASET('regress::csv-escaped-escape'+EmptyString, rec, CSV(ESCAPE('\\')));
OUTPUT(escaped4);

// Multi-escapes in a row
orig5 := DATASET([{'multiple escapes \\\\\\\\ in a row', 10, 'multiple at end \\\\\\\\'}], rec);
OUTPUT(orig5, ,'regress::csv-escaped-many'+EmptyString, OVERWRITE, CSV);
escaped5 := DATASET('regress::csv-escaped-many'+EmptyString, rec, CSV(ESCAPE('\\')));
OUTPUT(escaped5);

// Many escapes
orig6 := DATASET([{'many escapes like \\\'\\\' \\\'  \\\' and \\\\\\\\ \\\\ \\\\  \\\\  \\\\ escape', 10, 'escape at the end \\\''}], rec);
OUTPUT(orig6, ,'regress::csv-escaped-many-more'+EmptyString, OVERWRITE, CSV);
escaped6 := DATASET('regress::csv-escaped-many-more'+EmptyString, rec, CSV(ESCAPE('\\')));
OUTPUT(escaped6);

// Escape separator
orig7 := DATASET([{'escaping \\, the \\,\\, \\, \\, separator', 10, 'escape at the end \\,'}], rec);
OUTPUT(orig7, ,'regress::csv-escaped-separator'+EmptyString, OVERWRITE, CSV);
escaped7 := DATASET('regress::csv-escaped-separator'+EmptyString, rec, CSV(ESCAPE('\\')));
OUTPUT(escaped7);

// Escape with quotes
orig8 := DATASET([{'\'escaping\'\'the quote\'', 10, 'au naturel'}], rec);
OUTPUT(orig8, ,'regress::csv-escaped-escaped'+EmptyString, OVERWRITE, CSV);
escaped8 := DATASET('regress::csv-escaped-escaped'+EmptyString, rec, CSV(QUOTE('\'')));
OUTPUT(escaped8);

// Escape with quotes with ESCAPE()
orig9 := DATASET([{'\'escaping\'\'the quote\'', 10, 'with user defined escape'}], rec);
OUTPUT(orig9, ,'regress::csv-escaped-escaped2'+EmptyString, OVERWRITE, CSV);
escaped9 := DATASET('regress::csv-escaped-escaped2'+EmptyString, rec, CSV(ESCAPE('\\'), QUOTE('\'')));
OUTPUT(escaped9);

// Default is no escape
//NOTE: Blank lines are stripped by the regression suite code, so ensure each line has a !
orig10 := DATASET([
    {'this is a line with new lines \n!\n in it', 10, 'while this is not'},
    {'this is a line with new lines \r\n!\r\n in it', 10, 'while this is not'},
    {'this is a line with "quotes" \n!\n in it', 10, 'while this is not'},
    {'',0,''}
    ], rec);
OUTPUT(orig10, ,'regress::csv-orig10'+EmptyString, OVERWRITE, CSV(QUOTE('"')));
escaped10 := DATASET('regress::csv-orig10'+EmptyString, rec, CSV(QUOTE('"'),MAXLENGTH(1)));
OUTPUT(escaped10);
