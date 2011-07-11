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

r := record
unsigned        id;
boolean         t1;
boolean         t2;
boolean         t3;
boolean         t4;
boolean         t5;
boolean         t6;
boolean         t7;
boolean         t8;
boolean         t9;
boolean         t10;
boolean         t11;
boolean         t12;
boolean         t13;
boolean         t14;
boolean         t15;
boolean         t16;
boolean         t17;
boolean         t18;
boolean         t19;
boolean         t20;
boolean         t21;
boolean         t22;
boolean         t23;
boolean         t24;
boolean         t25;
boolean         t26;
boolean         t27;
boolean         t28;
boolean         t29;
        end;

ds := dataset('ds', r, thor);

addup(boolean x) := if(x, 1, 0);


r2 := record
unsigned        id;
unsigned1       score;
        end;

r2 t(r l) := transform
self.id := l.id;
self.score := 
        addup(l.t1) + addup(l.t2) + addup(l.t3) + addup(l.t4) + addup(l.t5) + addup(l.t6) + addup(l.t7) + addup(l.t8) + addup(l.t9) +
        addup(l.t10) + addup(l.t11) + addup(l.t12) + addup(l.t13) + addup(l.t14) + addup(l.t15) + addup(l.t16) + addup(l.t17) + addup(l.t18) + addup(l.t19) +
        addup(l.t20) + addup(l.t21) + addup(l.t22) + addup(l.t23) + addup(l.t24) + addup(l.t25) + addup(l.t26) + addup(l.t27) + addup(l.t28) + addup(l.t29);

    end;

output(project(ds, t(LEFT)));

    
