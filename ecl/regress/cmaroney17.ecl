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

dobrec := record
  unsigned id;
  string8  dob1;
  string8  dob2;
  string8  dob3;
end;

singledob := record
  string8 dob;
end;

maindata := record
   unsigned id;
   dataset(singledob) mydobs;
end;

main := dataset('main', maindata, thor);
dobs := dataset('dobs', dobrec, thor);

maindata myjoin(main L, dobs R) := transform
   self.mydobs := dataset([{R.dob1},{R.dob2},{R.dob3}],singledob);
   self := l;
end;


output(join(main, dobs, left.id = right.id, myjoin(left, right)));

