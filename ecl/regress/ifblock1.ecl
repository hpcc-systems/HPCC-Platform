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

a := record
            unsigned1 val1;
end;

b := record
            unsigned1 vala;
            unsigned1 valb;
end;

c := record
            unsigned1 val3;
            unsigned1 val4;
            unsigned1 val5;
end;

r := record
            string1 code;
            ifblock (self.code = 'A')
                        a;
            end;
            ifblock (self.code = 'B')
                        b;
            end;
            ifblock (self.code = 'C')
                        c;
            end;
end;

x := dataset ([{'A',1},{'B',1,2},{'C',1,2,3}],r);

OUTPUT (X);
