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

import lib_stringlib;

string const_a := 'a' : stored('const_a');

testStringlib := MODULE
  EXPORT AaaStartup := OUTPUT('Begin test');

  EXPORT Test0 := 'This should not be output by evaluate';  

   EXPORT Test11 := ASSERT(stringLib.StringCompareIgnoreCase('A', 'a') = 0, const_a + ' doesn\'t match "A"', const);    //oh no it isn't
  
  EXPORT ZzzClosedown := OUTPUT('End test');
END;

testAllPlugins := MODULE
  EXPORT testStringLib := testStringLib;
END;

EVALUATE(testAllPlugins);
