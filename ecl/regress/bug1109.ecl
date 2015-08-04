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

person := dataset('person', { unsigned8 person_id }, thor);
anytablename := person;

assignx(t) := macro
    self.x := 99;
endmacro;

r:= record
    x := 0;
    y := 1;
    z := 2;
end;

r tra(anytablename l) := transform
    self.y := 99;
    assignx(t)
    self.z := 1;
end;

c := project(anytablename, tra(left));

output(c);



