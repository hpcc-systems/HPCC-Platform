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

fixedRecord := 
        RECORD
string20            forename;
string20            surname;
string2             nl := '\r\n';
        END;

variableRecord := 
        RECORD
string              forename;
string              surname;
string2             nl := '\r\n';
        END;

fixedRecord var2Fixed(variableRecord l) :=
    TRANSFORM
        SELF := l;
    END;

variableRecord fixed2Var(fixedRecord l) :=
    TRANSFORM
        SELF.forename := TRIM(l.forename);
        SELF.surname := TRIM(l.surname);
        SELF := l;
    END;

d := PIPE('pipeRead 20000 20', fixedRecord);
output(d,,'dtfixed'); 


d2 := PROJECT(d, fixed2Var(LEFT));
output(d2,,'dtvar'); 
