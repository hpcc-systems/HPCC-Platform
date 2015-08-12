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
string5 zip;
string1 other;
end;

ds:=dataset('xx',rec,flat);

rec tra(rec l, string5 zipx) := transform
  self.zip := zipx+'xx';
  self := l;
end;

//new_ds := project(ds, tra(left, left.zip));

new_ds := project(ds, tra(left, ds.zip));

output(new_ds);