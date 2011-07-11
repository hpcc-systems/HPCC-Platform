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

rec1 := record
    string20    ReferenceCode := '1';
    string20    BillingCode := '2';
end;

rec2 := record
rec1        child1;
unsigned8   value;
        end;

rec3 := record
unsigned8   value;
rec2        child2;
        end;


ds := dataset('ds', rec3, thor);

rec3.child2.child1 t(rec3 l) := transform
    self := l.child2.child1;
    end;

output(project(ds, t(LEFT)));

