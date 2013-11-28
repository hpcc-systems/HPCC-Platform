/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2013 HPCC Systems.

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

IMPORT SqLite3;

childrec := RECORD
   string name => unsigned value;
END;

dataset(childrec) testSqLite(unsigned lim) := EMBED(SqLite3 : file('test.db'))
  SELECT * from tbl1 where two = :lim;
ENDEMBED;

dataset(childrec) testSqLite2(string v) := EMBED(SqLite3 : file('test.db'))
  SELECT * from tbl1 where one = :v;
ENDEMBED;

unsigned testSqLite3(string v) := EMBED(SqLite3 : file('test.db'))
  SELECT count(*) from tbl1 where one = :v;
ENDEMBED;

output(testSqLite(20));
output(testSqLite2('hello!'));
output(testSqLite3('hello!'));
