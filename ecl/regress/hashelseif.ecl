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




textToString(value) := macro

#if (value<=3)
    #if (value=1)
'One'
    #elseif (value=2)
'Two'
    #else
'Three'
    #end
#elseif (value<=10)
'Single digit'
#elseif (value<=100)
    #if (value < 20)
'Teens'
    #elseif (value <50)
'Low Double digit'
    #else
'High Double digit'
    #end
#else
'Very large'
#end

endmacro;

textToString(1);
textToString(2);
textToString(3);
textToString(8);
textToString(13);
textToString(25);
textToString(67);
textToString(1000);
