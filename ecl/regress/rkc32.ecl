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

d := dataset([{1}, {2}], {unsigned f});

string tostr(unsigned i) := CASE(i, 1=>'one', 2=>'two', 'lots');

boolean stringthem := false : stored('stringthem');

outformat1 := RECORD
         unsigned f;
             END;

outformat2 := RECORD
         string10 s;
             END;

outformat2 dst(d l) := TRANSFORM
  self.s := tostr(l.f);
end;

outformat1 dnst(d l) := TRANSFORM
  self.f := l.f;
end;

dstringed := project(d, dst(LEFT));
dnotstringed := project(d, dnst(LEFT));

if (stringthem, output(dstringed), output(dnotstringed));
