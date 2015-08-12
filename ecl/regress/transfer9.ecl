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

foo := record
  string20 line;
end;
dsfoo := nofold(dataset([{'12345678901234567890'}],foo));
bar := record
     string5 a;
     string5 b;
     string5 c;
     string5 d;
end;

bar tfoo(foo l) := transform
  self := transfer(l,typeof(bar));
end;
dsbar := project(dsfoo,tfoo(left));

output(dsbar);

