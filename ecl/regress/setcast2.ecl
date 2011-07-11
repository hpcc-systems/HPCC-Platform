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

set of integer2 setOfSmall := [] : stored('setOfSmall');
set of integer4 setOfMid := [] :  stored('setOfMid');

boolean useSmall := false : stored('useSmall');

set of integer4 stored4 := setOfSmall : stored('stored4');

set of integer stored8 := if(useSmall, setOfSmall, setOfMid) : stored('stored8');

output(99 in stored4);
output(99 in stored8);



z := record
set of integer8 x := stored4;
     end;

ds := dataset([{stored4}], z);
output(ds);
