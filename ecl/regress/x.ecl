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

//#workunit ('protect',true);

r := record,maxlength(1000)
  string xx;
  end;

xx := dataset(
[
{'abcdefghijklmn'},
{'1abcdefghijklmn'},
{'2abcdefghijklmn'},
{'3abcdefghijklmn'},
{'4abcdefghijklmn'},
{'abcdefghijklmnopqrstuvwxyz'}
], r);

output(xx,,'out.d00',overwrite);
