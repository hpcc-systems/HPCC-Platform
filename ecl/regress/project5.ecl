/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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

#option ('globalOptimize', 1);

ppersonRecord := RECORD
string10    surname ;
string10    forename;
string2     nl;
  END;


ppersonRecordEx := RECORD
            ppersonRecord;
unsigned4   id;
unsigned1   id1;
unsigned1   id2;
unsigned1   id3;
    END;


pperson := DATASET('in.d00', ppersonRecord, FLAT);


ppersonRecordEx projectFunction(ppersonRecord incoming) :=
    TRANSFORM
        integer rand := random();
        SELF.id := rand;
        SELF.id1 := 0;
        SELF.id2 := 0;
        SELF.id3 := 0;
        SELF := incoming;
    END;


ppersonEx := project(pperson, projectFunction(left));

ppersonRecordEx projectFunction2(ppersonRecordEx l) :=
    TRANSFORM,skip(l.id=0)
        integer1 rangex(unsigned4 divisor) := (l.id DIV divisor) % 100;
        SELF.id1 := rangex(10000);
        SELF.id2 := rangex(100);
        SELF.id3 := rangex(1);
        SELF := l;
    END;


ppersonEx2 := project(ppersonEx, projectFunction2(left));

output(ppersonEx2(id2>0), , 'out.d00');
