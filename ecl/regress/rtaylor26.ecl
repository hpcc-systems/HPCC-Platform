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


namesRec := RECORD
  STRING20       lname;
  STRING10       fname;
  UNSIGNED2    age := 25;
  UNSIGNED2    ctr := 0;
END;

namesTable2 := DATASET([{'Flintstone','Fred',35},

                                                {'Flintstone','Wilma',33},

                                                {'Jetson','Georgie',10},

                                                {'Mr. T','Z-man'}], namesRec);

 

loopBody(DATASET(namesRec) ds, unsigned4 c) :=

            PROJECT(ds, 

                          TRANSFORM(namesRec, 

                                                SELF.age := LEFT.age+c; 

                                                SELF.ctr := COUNTER ;

                                                SELF := LEFT));

 
g1 := GRAPH(namesTable2,10,loopBody(ROWSET(LEFT)[COUNTER-1],COUNTER));
output(g1);
