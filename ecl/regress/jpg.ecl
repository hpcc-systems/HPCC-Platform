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

JPEG(INTEGER len) := TYPE

            EXPORT DATA LOAD(DATA D) := D[1..len];

            EXPORT DATA STORE(DATA D) := D[1..len];

            EXPORT INTEGER PHYSICALLENGTH(DATA D) := len;

END;



export Layout_imgdb := RECORD, MAXLENGTH(50000)

            STRING1 ID_L;

            UNSIGNED5 ID_N;

            UNSIGNED2 DATE;

            UNSIGNED2 LEN;

            JPEG(SELF.LEN) PHOTO;

            UNSIGNED8 _FPOS { VIRTUAL(FILEPOSITION) };

END;




d := dataset('victor::imga', Layout_imgdb, flat);

// e := distribute(d, RANDOM());
// output(e,,'victor::imga.dist', overwrite);

output(choosen(d, 50));
