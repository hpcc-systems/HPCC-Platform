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

#option ('childQueries', true);

namesRecord :=
            RECORD
string20        surname;
string10        forename;
integer2        age := 25;
            END;

namesTable1 := dataset([
        {'Hawthorn','Mia',34},
        {'Hawthorn','Gavin',35}], namesRecord);

namesTable2 := dataset([
        {'Hawthorn','Nathan',2},
        {'Hawthorn','Abigail',2}], namesRecord);


combinedRecord :=
            record
unsigned        id;
dataset(namesRecord)    parents{maxcount(5)};
dataset(namesRecord, choosen(3))    children;
            end;

infile := dataset([0,1,2,3],{unsigned id});     // make sure the child queries are executed multiple times.

combinedRecord t1(infile l) :=
        TRANSFORM
            SELF := l;
            SELF.parents := global(namesTable1, few);
            SELF.children := global(namesTable2, many);
        END;

p1 := project(infile, t1(LEFT));
output(p1);


combinedRecord t2(infile l) :=
        TRANSFORM
            SELF := l;
            SELF.parents := sort(global(namesTable1, few), surname, forename);
            SELF.children := sort(global(namesTable2, many), surname, forename);
        END;

p2 := project(infile, t2(LEFT));
output(p2);

bestRecord :=
            record
unsigned        id;
namesRecord     firstParent;
namesRecord     firstChild;
            end;

bestRecord t3(infile l) :=
        TRANSFORM
            SELF := l;
            SELF.firstParent:= global(namesTable1[1], few);
            SELF.firstChild := global(namesTable2[1], many);
        END;

p3 := project(infile, t3(LEFT));
output(p3);

p4 := project(p2, transform(combinedRecord, self.id := 0; self := LEFT));
output(p4);

