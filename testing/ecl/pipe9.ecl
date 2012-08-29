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

import lib_stringlib;
import std.str;

mdstring(string del) := TYPE
  export integer physicallength(string s) := lib_stringlib.StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
  export string load(string s) := s[1..lib_stringlib.StringLib.StringUnboundedUnsafeFind(s,del)-1];
  export string store(string s) := s + del;
END;

// Test behavior if PIPE, REPEAT where the pipe returns no data
d := dataset([{'Line 1'}, {'Line 2'}, {'Line 3'}, {'Line 4'}, {'Line 5'},{'Line 6'},{'Line 7'},{'Line 8'}], { varstring20 line}) : stored('nofold');

dt(integer testNo) := TABLE(d, { mdstring('\n') l := line })(l != (STRING)testNo);   // Ensure that we don't gommon up the tests

p1 := PIPE(dt(1), 'grep "x"', { mdstring('\n') lout }, OPT);   // no records match
p2 := PIPE(dt(2), 'grep "x"', { mdstring('\n') lout }, REPEAT, OPT);
p3 := PIPE(dt(3), 'grep "x"', { mdstring('\n') lout }, REPEAT, GROUP, OPT);

p4 := PIPE(dt(4), 'grep "[138]"', { mdstring('\n') lout }, OPT);   // some records match, but gaps of length 1 and > 1
p5 := PIPE(dt(5), 'grep "[138]"', { mdstring('\n') lout }, REPEAT, OPT);
p6 := PIPE(dt(6), 'grep "[138]"', { mdstring('\n') lout }, REPEAT, GROUP, OPT);

p7 := PIPE(dt(7), 'grep "[3]"', { mdstring('\n') lout }, OPT);   // some records match, gap at start
p8 := PIPE(dt(8), 'grep "[3]"', { mdstring('\n') lout }, REPEAT, OPT);
p9 := PIPE(dt(9), 'grep "[3]"', { mdstring('\n') lout }, REPEAT, GROUP, OPT);

output(p1, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p2, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p3, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p4, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p5, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p6, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p7, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p8, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
'--------------------';
output(p9, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
