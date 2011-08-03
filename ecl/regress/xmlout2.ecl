/*##############################################################################

    Copyright (C) 2011 HPCC Systems.

    All rights reserved. This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU Affero General Public License as
    published by the Free Software Foundation, either version 3 of the
    License, or (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

