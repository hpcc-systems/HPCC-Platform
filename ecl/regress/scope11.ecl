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

//Deliberatly nasty to tie myself in knots.

countRecord := RECORD
unsigned8           xcount;
                END;

parentRecord := 
                RECORD
unsigned8           id;
boolean                isSpecial;
countRecord            xfirst;
DATASET(countRecord)   children;
                END;

parentDataset := DATASET('test',parentRecord,FLAT);


parentRecord ammend(parentrecord l) := TRANSFORM
    SELF.xfirst.xcount := if (l.isSpecial, l.xfirst, l.children[1]).xcount;
    SELF.children := [];
    SELF := l;
            END;

projected := project(parentDataset, ammend(LEFT));

output(projected);
