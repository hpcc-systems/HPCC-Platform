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

vrec := RECORD
INTEGER4 grp;
INTEGER4 id := 0;
INTEGER1 skp := 0;
INTEGER1 hasopt := 0;
IFBLOCK(SELF.hasopt>0)
INTEGER4 optqnt := 0;
END;
INTEGER4 qnt := SELF.id;
INTEGER4 cnt := 0;
END;

vset := DATASET([{1, 1}, {1, 2}, {1, 3, 0, 1, 99}, {1, 4, 0, 1, 1}, {2, 1}, {2, 2}, {2, 3}, {2, 4, 1}, {3, 1, 1}, {4, 1}, {4, 2, 1}, {4, 3}, {4, 4}], vrec);
output(vset);

