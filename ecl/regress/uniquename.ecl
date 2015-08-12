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

#option ('globalFold', false);
definex(t) := macro
  #uniquename(x)
  %x% := 3;
  t := %x%*2;
endmacro;

definex(t1)
t1;

definex(t2)
t2;

#uniquename(x)
%x% := 3;
t3 := %x%*2;
t3;

#uniquename(x)
%x% := 4;
t4 := %x%*5;
t4;



