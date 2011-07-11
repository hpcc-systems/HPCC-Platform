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





set of string mySet := ['one','two','three','four','five','six'];

searchSet := dataset(mySet, { string x{maxlength(50)} });


v1 := true : stored('v1');
v2 := 4 : stored('v2'); 

ds := dataset([{1},{2},{3},{4},{5},{6},{4},{6}],{ unsigned4 id });

r := record
    unsigned4 id;
    string value{maxlength(50)};
    string value1{maxlength(50)};
end;

r t(recordof(ds) l) := transform
    self.value := searchSet[l.id].x;
    self.value1 := IF(v1, searchSet[v2].x, searchSet[v2-1].x);
    self := l;
end;

p := project(nofold(ds), t(LEFT));

output(p);
