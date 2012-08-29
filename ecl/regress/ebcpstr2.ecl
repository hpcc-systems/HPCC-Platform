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

import dt;

r := record
   dt.ebcdic_pstring  a;
   end;

//d := dataset([{x'05'},{x'e6'},{x'C9'},{x'D5'},{x'C7'},{x'E2'}],r);

//output(d,,'temp.dab');

string load(ebcdic string x) := x[2..transfer(x[1], unsigned1)+1];

d := dataset('temp.dab',r,flat);

r1 := record
  string6 a;
  end;

r1 trans(r l) := transform
  self.a := l.a;
  end;

p := project(d,trans(left));

output(p)
