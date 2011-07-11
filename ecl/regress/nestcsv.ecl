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

//Convoluted test to check nested class in remote child is accessing the correct cursors.
#option ('targetClusterType', 'thor');

ds := dataset('ds', { string name, string sep }, thor);

linerec := { string line; };

outrec := record
dataset(linerec) lines;
        end;


outrec t(ds l) := transform

    self.lines := dataset(l.name, linerec, csv(separator(l.sep)));
    end;

output(project(ds, t(left)));
