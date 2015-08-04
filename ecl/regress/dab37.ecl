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


r := record
  ebcdic string10 a := 'hello';
  end;

r1 := record
  string10 a;
  end;

r1 trans(r l) := transform
  self.a := stringlib.stringextract(l.a,1);
  end;

d := dataset('jj',r,flat);

p := project(d,trans(left));

output(p,,'otu.d00');