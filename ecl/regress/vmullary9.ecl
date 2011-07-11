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

layout_u6 := RECORD
  unsigned6 dids;
END;

layout_1 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
END;
OUTPUT (sizeof (layout_1), NAMED ('sizeof_1'));

layout_2 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(1)};
END;
OUTPUT (sizeof (layout_2, MAX), NAMED ('sizeof_2'));

layout_3 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(1)};
  DATASET (layout_u6) id  {MAXCOUNT(1)};
END;
OUTPUT (sizeof (layout_3, MAX), NAMED ('sizeof_3'));

layout_4 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(2)};
END;
OUTPUT (sizeof (layout_4, MAX), NAMED ('sizeof_4'));

layout_5 := RECORD
  unsigned1 u1 := 0;
  string64  str64 := '';
  DATASET (layout_u6) verified {MAXCOUNT(2)};
  DATASET (layout_u6) id  {MAXCOUNT(2)};
END;
OUTPUT (sizeof (layout_5, MAX), NAMED ('sizeof_5'));
