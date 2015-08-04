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
unsigned    rand1;
unsigned    rand2;
unsigned    rand3;
    END;



ppersonRecordEx projectFunction(ppersonRecord l) := Transform
    SELF.rand1 := RANDOM()+1;
    SELF.rand2 := RANDOM()+1;
    SELF.rand3 := RANDOM()+1;
    SELF := l;
END;


pperson := DATASET('in.d00', ppersonRecord, FLAT);

ppersonEx := project(pperson, projectFunction(left));

output(ppersonEx, , 'out.d00');



