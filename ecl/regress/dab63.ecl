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

r := record,maxlength(2000)
            string f1;
            string f2;
            string f3;
            string f4;
            string f5;
            string f6;
            string f7;
            string f8;
            string f9;
            string f10;
            string f11;
            string f12;
            unicode f13;
  end;



d1 := dataset('~thor::in::docket_data_cr20041111',r,csv(separator('¦')));
output(d1);

d2 := dataset('~thor::in::docket_data_cr20041111',r,csv(separator(U'¦')));
output(d2);



