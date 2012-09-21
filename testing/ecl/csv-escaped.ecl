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

rec := RECORD
  string foo;
  integer id;
  string bar;
END;

// Default is no escape
orig := DATASET([{'this is an \\\'escaped\\\' string', 10, 'while this is not'}], rec);
OUTPUT(orig, , 'regress::csv-escaped', OVERWRITE, CSV);
escaped := DATASET('regress::csv-escaped', rec, CSV);
OUTPUT(escaped);

// Standard escape
orig2 := DATASET([{'this is an \\\'escaped\\\' string', 10, 'while this is not'}], rec);
OUTPUT(orig2, , 'regress::csv-escaped', OVERWRITE, CSV);
escaped2 := DATASET('regress::csv-escaped', rec, CSV(ESCAPE('\\')));
OUTPUT(escaped2);

// Multi-char escape
orig3 := DATASET([{'this is an -=-\'escaped-=-\' string', 10, 'while this is not'}], rec);
OUTPUT(orig3, , 'regress::csv-escaped', OVERWRITE, CSV);
escaped3 := DATASET('regress::csv-escaped', rec, CSV(ESCAPE('-=-')));
OUTPUT(escaped3);
