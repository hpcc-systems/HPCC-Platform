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

#option ('optimizeGraph', false);
#option ('foldAssign', false);
#option ('globalFold', false);

stringextract(const string src, integer x) := src[x..x+0];

namesRecord :=
            RECORD
string20        per_street;
string10        forename;
integer2        age := 25;
            END;

namesTable := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);


st :=
  RECORD
  namesTable.per_street;
  END;

ps := table(choosen(namesTable,100),st);

st remcom(st l) := transform
self.per_street :=(string45)stringextract(l.per_street,1)+' '+
(string45)stringextract(l.per_street,2)+' '+
(string45)stringextract(l.per_street,3);
self := l;
end;

p := project(ps,remcom(left));

output(p,,'out.d00')

/*
If you take the cast to string45 outside of the concats it works fine
....

st :=
  RECORD
  person.per_street;
  END;

ps := table(choosen(person,100),st);

st remcom(st l) := transform
self.per_street :=(string45)(stringlib.stringextract(l.per_street,1)+'
'+
stringlib.stringextract(l.per_street,2)+' '+
stringlib.stringextract(l.per_street,3));
self := l;
end;

p := project(ps,remcom(left));

output(p)
*/
