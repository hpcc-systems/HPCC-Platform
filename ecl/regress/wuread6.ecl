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

#option ('targetClusterType', 'roxie');
#option ('testLCR', true);

childRec := { unsigned6 id; };

rec := record
unsigned4       id;
string5         name;
dataset(childRec) ids{maxcount(10)} := _EMPTY_(childRec);
end;


names := dataset([
    {1, 'Gavin'},
    {2, 'Richard'},
    {3, 'Tony'},
    {4, 'Isaac'}], rec) : stored('in', few);
    
rec t(rec l) := transform
    self.id := l.id + names[1].id;
    self := l;
end;

p := project(names, t(LEFT));

output(p);
