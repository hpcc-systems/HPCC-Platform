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

#option ('targetClusterType', 'hthor');

EXPORT Layout_PairMatch := RECORD
  unsigned6 new_rid;
  unsigned6 old_rid;
  unsigned1  pflag;
END;

rec := record
    dataset(Layout_PairMatch) cd;
end;

ds := dataset([
{[{1600,405,0}]},
{[{1600,1350,0}]},
{[{405,1350,0}]},
{[{1350,433,0}]},
{[{433,1350,0}]}
],rec);


lilrec := recordof(ds);

bigrec := record, maxlength(10000)
    dataset(lilrec) cd;
end;

nada := dataset([{1}],{integer a});

bigrec makecd(nada l) := transform
    self.cd := ds;
end;

p2 := project(nada, makecd(left)) : global;
p := dataset('p', bigrec, thor);

output(p);
sum(p,  sizeof(p) );

//sizeof(ds)*count(ds);
