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

// this may cause memory leak
//  dd := 3;

  s1 := service
    integer dd() : entrypoint='dd';   //* redefiniton error although it should be ok. <== BUG
    integer ddx() : entrypoint='ddx';
    integer dd() : entrypoint='dd';
  end;

  s2 := service
    integer dd() : entrypoint='dd';   //* redefiniton error although it should be ok. <== BUG
    integer ddx() : entrypoint='ddx';  //* OK although it is already defined in s1
  end;

  ddx := 3; //* OK although it is already defined in s1 and s2

  s1.dd() + s2.dd();

  r := record
     integer1 dd;
     integer1 dd;
     integer1 ddx;
     integer1 r_f;
  end;

  r2 := record
     integer1 dd;
     integer1 ddx;
  end;

  r_f := 3;
