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

baserec := RECORD
     STRING6 name{xpath('thisisthename')};
     INTEGER8 blah{xpath('thisistheblah')};
     STRING9 value{xpath('thisisthevalue')};
END;

baseset := DATASET([{'fruit', 123, 'apple'}, {'fruit', 246, 'ford'}, {'os', 680, 'bsd'}, {'music', 369, 'rhead'}, {'os', 987, 'os'}], baserec);

o1 := output(baseset, , '~base.d00', OVERWRITE);

b1 := DATASET('~base.d00', baserec, FLAT);

sort0 := SORT(b1, name);

o2_1 := output(choosen(sort0, 50) , , '~out2_2.d00', OVERWRITE);

o2_2 := output(choosen(sort0, 100) , , '~out2_1.d00', OVERWRITE);

o2_3 := output(choosen(sort0, 150) , , '~out2_3.d00', OVERWRITE);

o2_4 := output(choosen(sort0, 200) , , '~out2_4.d00', OVERWRITE);

PARALLEL(o2_1, o2_2, o2_3, o2_4);
