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

r := record
  string200 s;
  end;
d := dataset([{'KEYA:Fred; KEYB:Edith KEYC:John'}],r);

PATTERN labels := ['KEYA:','KEYB:','KEYC:'];
PATTERN termin := (';' OR '||' OR ' ');
PATTERN termination :=  (termin+ before ( labels or last )) or last;

//PATTERN val1 := ANY+ before labels;
//PATTERN val2 := ANY+ before ('||' or last);
pattern body := (ANY not in termin)+;
PATTERN items := opt(labels) body termination;


r1 := record
  string res := matchtext(body);
  end;

p := parse(d,s,items,r1,min,many);

output(p);
