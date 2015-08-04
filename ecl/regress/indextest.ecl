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

baserec := RECORD
    STRING6 name;
    INTEGER6 blah;
    STRING9 value;
END;

baseset := DATASET([{'fruit', 123, 'apple'}, {'music', 456, 'aphex'}, {'os', 789, 'linux'}, {'car', 246, 'ford'}, {'os', 468, 'win32'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'book', 987, 'cch22'}], baserec);

genbase := OUTPUT(baseset, , '~bug15197.d00', OVERWRITE);

fpbaserec := RECORD
    baserec;
    UNSIGNED8 filepos{virtual(fileposition)};
END;

fpbaseset := DATASET('~bug15197.d00', fpbaserec, FLAT);

genidx := BUILDINDEX(fpbaseset, {name, filepos}, '~bug15197.idx', OVERWRITE);

goodidx := INDEX(fpbaseset, {name, filepos}, '~bug15197.idx');

badidx := INDEX(fpbaseset, {blah, filepos}, '~bug15197.idx');

SEQUENTIAL(genbase, genidx, OUTPUT(goodidx), OUTPUT(badidx));

