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

ppersonRecord := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;
ppersonRecord2 := RECORD
string3     id := '000';
string10    surname := '';
string10    forename := '';
unsigned1 nl1 := 13;
unsigned1 nl2 := 10;
  END;

pperson1 := DATASET('in.d00', ppersonRecord, THOR);

tmptable := dataset([
        {'001', 'Hawthorn','Gavin', 13, 10},
        {'002', 'Smith','Zack', 13, 10},
        {'003', 'Hewit','Nigel', 13, 10},
        {'004', 'Gillin','Paul', 13, 10}], ppersonRecord);


ppersonRecord2 tr1 (ppersonRecord l)
:=
    transform
                   self := l;
    end;

ppersonRecord JoinTransform (ppersonRecord l, ppersonRecord2 r)
:=
    transform
                   self := l;
                   self := r;
    end;



s1 := SORT(pperson1, surname);

ppersonRecord tr2 (ppersonRecord l, ppersonRecord r)
:=
    transform
                   self.nl1 := l.nl1 + 5;
           SELF := r;
    end;

g1 := group(s1, surname);

it := iterate(s1, tr2(LEFT, RIGHT));

s2 := sort(group(g1), forename);

dg := group(it);

hd := dedup(dg, forename, ALL);

output(hd);
output(s2);
