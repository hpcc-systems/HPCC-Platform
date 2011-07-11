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

childPersonRecord := RECORD
string20          forename;
unsigned1         age;
    END;

personRecord := 
                RECORD
string20            forename;
string20            surname;
DATASET(childPersonRecord)   children;
                END;


personDataset := nofold(DATASET(
            [{'Gavin','Halliday',[{'Abigail',2},{'Nathan',2}]},
             {'John','Simmons',[{'Jennifer',18},{'Alison',16},{'Andrew',13},{'Fiona',10}]}],personRecord));


//Global if

bool trueValue := true : stored('trueValue');
bool falseValue := false : stored('falseValue');

output(IF(trueValue,personDataset));
output(IF(falseValue,personDataset));


//Out of line if
personRecord t(personRecord l) := transform
    self.children := IF(l.forename = 'Gavin', sort(l.children, forename));
    self := l;
end;

output(t(personDataset));


//Inline if

personRecord t2(personRecord l) := transform
    self.children := IF(l.forename = 'Gavin', l.children);
    self := l;
end;

output(t2(personDataset));

