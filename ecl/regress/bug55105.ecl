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

layout := {
   integer i1,
   string  s2,
   unicode u3,
};

ds := dataset([{1,'a',u'z'},{2,'b',u'y'}], layout);

layout throwError(layout rec) := transform

   self.i1 := if(rec.i1 != 0,   rec.i1, error('message1'));
   self.s2 := if(rec.s2 != 'c', rec.s2, error('message2'));

   // Without the cast to unicode, syntax check will fail with
   // "Error: Incompatible types: expected unicode, given "
   self.u3 := if(rec.u3 != u'x', rec.u3, (unicode) error('message3'));

end;

ds2 := project(ds, throwError(left));
output(ds2);
