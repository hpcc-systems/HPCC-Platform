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




outputRow(showA = false, showB = false, showC = false, aValue = '\'abc\'', bValue = 10, cValue = true) := macro
    output(IF(showA,' a='+aValue,'')+if(showB,' b='+(string)bValue,'')+if(showc,' c='+(string)cValue,''))
    endmacro;

//Some of these aren't currently caught

outputRow(showZ := 100);                            // unknown parameter
outputRow(showB := true, showB := true);        // already given a value
outputRow(aValue := 'Changed value', true);         // value after named
outputRow(false,,,'Changed value2',showA := true);  // already given a value

outputRow(named showZ := 100);                          // unknown parameter
outputRow(named showB := true, named showB := true);        // already given a value
outputRow(named aValue := 'Changed value', true);           // value after named
outputRow(false,,,'Changed value2',named showA := true);    // already given a value
