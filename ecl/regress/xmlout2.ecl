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

sys := service
  string8 GetDateYYYYMMDD() : c,once,entrypoint='slGetDateYYYYMMDD2', hole;
END;

dateNow := sys.GetDateYYYYMMDD();

namesRecord :=
            RECORD
string20        surname;
string10        forename{xpath('foreName')};
integer2        age := 25;
            END;

namesTable2 := dataset([
        {'Hawthorn','Gavin',31},
        {'Hawthorn','Mia',30},
        {'Smithe','Pru',10},
        {'X','Z'}], namesRecord);

output(namesTable2,,'out0.xml',overwrite,xml);
output(namesTable2,,'out1.xml',overwrite,xml(heading('<?xml version=1.0 ...?>\n<qlrs date=\'' + (string)dateNow + '\'>\n', '</qlrs>')));
output(namesTable2,,'out2.xml',overwrite,xml('qlr'));
output(namesTable2,,'out3.csv',overwrite,csv(heading('--- Start of csv file ---\n', '--- End of csv file ----')));
output(namesTable2,,'out4.csv',overwrite,csv);
output(namesTable2,,'out5.xml',overwrite,xml(heading('<?xml version=1.0 ...?>\n<Dataset>\n')));

