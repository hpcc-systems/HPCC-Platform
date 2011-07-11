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

//I'm really not sure about this syntax for a scope resolution operator.....

export valueE := 1;
valueP := 2;

m := module

    export valueE := 10;

    integer valueP := 20;

    export m := module

        export valueE := 100;

        integer valueP := 200;

        export m := module

            export valueE := 1000;

            integer valueP := 2000;

            export m := module

                export valueE := 10000;

                integer valueP := 20000;

                export vP1 := valueP;
                export vE1 := valueE;
                export vP2 := ^.valueP;
                export vE2 := ^.valueE;
                export vP3 := ^^.valueP;
                export vE3 := ^^.valueE;
                export vP4 := ^^^.valueP;
                export vE4 := ^^^.valueE;
                export vP5 := ^^^^.valueP;
                export vE5 := ^^^^.valueE;
                export vP6 := ^^^^^.valueP;     // too many
                export vE6 := ^^^^^.valueE;
            end;
        end;
    end;
end;

output(m.m.m.m.vp1);
output(m.m.m.m.ve1);
output(m.m.m.m.vp2);
output(m.m.m.m.ve2);
output(m.m.m.m.vp3);
output(m.m.m.m.ve3);
output(m.m.m.m.vp4);
output(m.m.m.m.ve4);
output(m.m.m.m.vp5);
output(m.m.m.m.ve5);
