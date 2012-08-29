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

export mdstring(string del) := TYPE  
  export integer physicallength(string s) := lib_stringlib.StringLib.StringUnboundedUnsafeFind(s,del)+length(del)-1;
  export string load(string s) := s[1..lib_stringlib.StringLib.StringUnboundedUnsafeFind(s,del)-1];
  export string store(string s) := s + del;
END;

d := dataset([{ 'Hello there'}, {'what a nice day'}, {'1234'}], { varstring20 line}) : stored('nofold');

dt := TABLE(d, { mdstring('\n') l := line });

p1 := PIPE(dt(l!='p1'), 'sort', { mdstring('\n') lout });
p2 := PIPE(dt(l!='p2'), 'sort', { mdstring('\n') lout }, REPEAT);
p3 := CHOOSEN(PIPE(dt(l!='p3'), 'sort', { mdstring('\n') lout }), 1);
p4 := CHOOSEN(PIPE(dt(l!='p4'), 'sort', { mdstring('\n') lout }, REPEAT), 1);

output(p1, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p2, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p3, { string20 l := Str.FindReplace(lout, '\r', ' ') } );
output(p4, { string20 l := Str.FindReplace(lout, '\r', ' ') } );


