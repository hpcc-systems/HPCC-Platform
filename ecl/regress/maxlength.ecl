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

  r1 := record
  string f1{maxlength(99)}
    end;

r2 := record
  r1;
  unsigned4 id;
  end;


t1 := dataset('d1', r1, thor);
output(t1);

t2 := dataset('d2', r2, thor);
output(t2);

r3 := record
    t2.f1;
    f1a := t2.f1;
    f1b{maxlength(10)} := t2.f1;
    end;

t3 := dataset('d3', r3, thor);
output(t3);

