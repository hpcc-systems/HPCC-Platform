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

pattern patTerminator := ['.','?',LAST];

pattern sentance := repeat(any,min) patTerminator;

infile := dataset([
        {'One.Two?Three.'},
        {'Not finished'}
        ], { string line });


results :=
    record
        MATCHTEXT;
    end;

output(PARSE(infile,line,sentance,results,first,scan));     // One. Two? Three.
output(PARSE(infile,line,sentance,results,first,scan all)); // One. ne. e. . Two? wo? o? ? Three. hree. ree. ee. e. .
output(PARSE(infile,line,sentance,results,noscan,all));     // One. One.Two? One.Two?Three
output(PARSE(infile,line,sentance,results,scan,all));   // One. One.Two? One.Two?Three Two? Two?Three Three

