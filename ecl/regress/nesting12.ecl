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

rec1 := record
string10 fld1;
string10 fld2;
string10 fld3;
end;

rec2 := record
rec1 one;
rec1 two;
rec1 three;
end;

rec2 xform(rec2 L) := transform
self.one := [];
self.two := [];
self.three := [];
end;

x := dataset('x', rec2, thor);
y := project(x, xform(LEFT));
output(y);