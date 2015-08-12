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
string10    surname ;
string10    forename;
string2     nl;
  END;


ppersonRecordEx := RECORD
string10    surname ;
string10    forename;
integer1    age;
string1     sex;
string2     nl;
unsigned4   seq;
    END;



ppersonRecordEx projectFunction(ppersonRecord incoming, unsigned4 c, string1 sex) := Transform
    SELF.age := 33;
    SELF.sex := sex;
    SELF.seq := c;
    SELF := incoming;
END;


pperson := DATASET('in.d00', ppersonRecord, FLAT);

ppersonEx := project(pperson, projectFunction(left, COUNTER, 'M'));

f := ppersonEx(age != 10);
output(f, , 'out.d00');

ppersonEx2 := project(pperson, projectFunction(left, COUNTER, 'F'));

f2 := ppersonEx2(seq != 10);
output(f2, , 'out.d00');

ppersonEx3 := project(pperson, projectFunction(left, COUNTER, 'N'));
f3 := ppersonEx3(age = 10);
output(f3, , 'out.d00');

