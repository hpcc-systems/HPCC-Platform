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

#option ('globalFold', false);

datalibx := service
    string141 addressclean(const string src, const string filter) : eclrtl,library='dab',entrypoint='rtlStringFilter';
end;


Address1 := '41 S TRINGO DRIVE #104';
Address2 := 'DELRAY BEACH FL 33445';

Clean_Address := datalibx.addressclean(Address1,Address2);

d := dataset('~local::rkc::person', { varstring15 name, varunicode10 id, unsigned8 filepos{virtual(fileposition)} }, flat);
i := index(d, {d}, '~key.person');

a1 := i((name in [V'RICHARD',(varstring)(Clean_Address[1..10])]) and id = (varunicode)U'ABC');
output(a1);

a2 := i((name in [(varstring)(Clean_Address[1..10])]));
output(a2);
a3 := i((name = ((varstring)Clean_Address)[1..10]));
output(a3);

