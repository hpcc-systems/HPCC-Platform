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

import lib_logging;

ppersonRecord := RECORD
string10    surname ;
string10    forename;
string2     nl;
  END;


ppersonRecordEx := RECORD
string10    surname ;
string10    forename;
integer1    age;
unsigned1   sex;        // 1 == male, 0 == female
string2     nl;
    END;


warnValue(string text, integer value) := function
    logging.addWorkunitWarning(text);
    return value;
end;

ppersonRecordEx projectFunction(ppersonRecord incoming) := Transform
    SELF.age := warnValue('Help',33);
    SELF.sex := if(incoming.surname='',warnValue('Invalid: '+incoming.forename,1),2);

    SELF := incoming;
END;


pperson := DATASET([{'Hawthorn','Gavin',''},{'','James',''}], ppersonRecord);

ppersonEx := project(pperson, projectFunction(left));

output(ppersonEx);



