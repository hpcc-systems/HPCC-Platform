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

thina :=     RECORD
unsigned4       f1;
string20        f2;
            END;

thin :=     RECORD
                thina;
                ifblock(self.f1 > 10)
qstring10           f3;
                end;
            END;

fat :=      RECORD
                thin;
string30        x2;
thina           g1;
unsigned        temp;
unsigned        g3;
                ifblock(self.g3 > 100)
unsigned            g4;
                end;
            END;

inFat := dataset('fat', fat, thor);


output(ebcdic(inFat));
output(ascii(inFat));
