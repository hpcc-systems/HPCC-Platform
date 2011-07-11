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

//NoThor
//Test for bug 25636

namesRecord := 
            RECORD
string20        surname;
            END;

idRecord := record
boolean include;
dataset(namesRecord) people{maxcount(20)};
    end;


ds := dataset([
        {false,[{'Gavin'},{'Liz'}]},
        {true,[{'Richard'},{'Jim'}]},
        {false,[]}], idRecord);

idRecord t(idRecord l) := transform
    sortedPeople := sort(l.people, surname);
    self.people := if(not l.include, sortedPeople(l.include)) + sortedPeople;
    self := l;
end;

output(project(ds, t(left)));
