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


rec := record
   string1 K1 := '!';
   string1 K2 := '!';
   string3 N := '0';
   string1 EOL := x'0a';
end;

rec TR(rec l,rec r) := TRANSFORM
   SELF.K1 := r.K1;
   SELF.K2 := r.K2;
   SELF.N := (string3)((integer)l.N + (integer)r.N);
   SELF.EOL := r.EOL;
end;


DS1 := dataset('testddru', rec, flat);
DS2 := dataset('testddrj', rec, flat);
J := join (DS1, DS2, LEFT.K1 = RIGHT.K1, TR (LEFT, RIGHT));

output(J,,'testddrj.out');


