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

myRecord := RECORD
    UNSIGNED4 uid;
END;

doAction(set of dataset(myRecord) allInputs) := function
    set of integer emptyIntegerSet := [];
    inputs := RANGE(allInputs, emptyIntegerSet);
    return JOIN(inputs, stepped(left.uid = right.uid), transform(left), sorted(uid));
end;
nullInput := dataset([1,2,3], myRecord);

s := GRAPH(nullInput, 0, doAction(rowset(left)));

output(s);
