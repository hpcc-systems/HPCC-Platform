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

in1rec := RECORD
    UNSIGNED1 id;
    BITFIELD4 age1;
    BITFIELD4 age2;
END;

in1 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in1rec);

output(in1);

in2rec := RECORD
    UNSIGNED1 id;
    BITFIELD4_8 age1;
    BITFIELD4_8 age2;
END;

in2 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in2rec);

output(in2);

in3rec := RECORD
    UNSIGNED1 id;
    BITFIELD4 age1;
    BITFIELD12 age2;
END;

in3 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in3rec);

output(in3);

in4rec := RECORD
    UNSIGNED1 id;
    BITFIELD4_2 age1;
    BITFIELD12_2 age2;
END;

in4 := DATASET([{1,10,12},
            {2,4,8},
            {3,3,6}], in4rec);

output(in4);
