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

layout_city := record
unsigned4 UniqueID;
string25    city;
end;

my_cities := dataset([{1,'Boynton Beach'},{1,'Cincinnati, OH'}], layout_city);

layout_names := record
unsigned4 UniqueID;
string20    fname;
string20    mname;
set of string cities;
end;

names := dataset([{1,'Tony','Middleton',[]}], layout_names);

// Denormalize cities
layout_names AppendCities(names l, my_cities r) := transform
    self.cities := l.cities + [r.city];
    self := l;
end;

names_with_cities := denormalize(names,
                                 my_cities,
                                 left.UniqueID = right.UniqueID,
                                 AppendCities(left, right));

output(names_with_cities);
