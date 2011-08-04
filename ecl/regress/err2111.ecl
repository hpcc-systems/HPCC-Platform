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

NamesRec := RECORD
    //STRING20  thename;
    STRING20  addr1 := '';
    STRING20  addr2 := '';
END;

OutRec := RECORD
    STRING20  thename;
    STRING20  addr;
END;


OutRec Trans(NamesRec L, INTEGER C) :=
    TRANSFORM
      SELF := L;
      SELF.addr := CHOOSE(C, L.addr1, L.addr2);
    END;
