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

aaa := DATASET('aaa',{STRING1 fa; }, FLAT);
bbb := DATASET('bbb',{STRING1 fb; }, FLAT);

diff := aaa - bbb;
joins := aaa * bbb;
sum := aaa + bbb;

xx := JOIN(3,4);

ResultRec := RECORD 
    INTEGER fa;
    END;

ResultRec Trans(aaa L, bbb R) := TRANSFORM
    SELF := L;
        //SELF.x := SELF.x + 'a';
    END;
