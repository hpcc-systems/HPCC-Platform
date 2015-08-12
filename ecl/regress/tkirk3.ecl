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

import dt;

NameRecord :=
record
  string5    title;
  string20   fname;
  string20   mname;
  string20   lname;
  string5    name_suffix;
  string3    name_score;
end;

LocalAddrCleanLib := SERVICE
NameRecord dt(const string name, const string server = 'x') : c,entrypoint='aclCleanPerson73',pure;
END;


MyRecord :=
record
  unsigned id;
  string uncleanedName;
  NameRecord   Name;
end;

x := LocalAddrCleanLib.dt('Gavin Hawthorn') : global;

output(x.lname);


output(LocalAddrCleanLib.dt('Jason Hawthorn').fname);
output(LocalAddrCleanLib.dt('Jason Hawthorn').name_score);

