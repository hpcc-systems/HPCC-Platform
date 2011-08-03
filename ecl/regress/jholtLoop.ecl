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
    STRING20 lname;
    STRING10 fname;
    UNSIGNED2 age := 25;
    UNSIGNED2 ctr := 0;
END;
namesTable2 := DATASET([{'Flintstone','Fred',35},
                                                {'Flintstone','Wilma',33},
                                                {'Jetson','Georgie',10},
                                                {'Mr. T','Z-man'}], namesRec);
loopBody(DATASET(namesRec) ds, unsigned4 c) :=
                                PROJECT(ds,
                                                TRANSFORM(namesRec,
                                                                    SELF.age := LEFT.age*c;
                                                                    SELF.ctr := COUNTER ;
                                                                    SELF := LEFT));

//Form 1:
OUTPUT(LOOP(namesTable2, 4, PROJECT(ROWS(LEFT),
                                                                        TRANSFORM(namesRec,
                                                                                             SELF.ctr:=COUNTER,
                                                                                             SELF:=LEFT))
                                                    & PROJECT(ROWS(LEFT),
                                                                         TRANSFORM(namesRec,
                                                                                             SELF.ctr:=COUNTER*10,
                                                                                             SELF:=LEFT))),
             NAMED('Form_1_Count'));

//Form 2:
OUTPUT(LOOP(namesTable2,
                        10,
                        LEFT.age * COUNTER <= 200,
                        PROJECT(ROWS(LEFT),
                                        TRANSFORM(namesRec,
                                                            SELF.age := LEFT.age*2;
                                                            SELF.ctr := COUNTER,
                                                            SELF := LEFT))),
                NAMED('Form_2_Count_and_Filter'));

// Form 3
OUTPUT(LOOP(namesTable2,
                        LEFT.age < 200,
                        loopBody(ROWS(LEFT), COUNTER)),
                NAMED('Form_3_Loop_Filter'));

//Form 4:
OUTPUT(LOOP(namesTable2,
                        SUM(ROWS(LEFT), age) < 1000 * COUNTER,
                        PROJECT(ROWS(LEFT),
                                        TRANSFORM(namesRec,
                                                            SELF.age := LEFT.age*2;
                                                            SELF.ctr := COUNTER,
                                                            SELF := LEFT))),
                NAMED('Form_4_Condition_ChildQ_ROWS'));

OUTPUT(LOOP(namesTable2,
                        COUNTER <= 10,
                        PROJECT(ROWS(LEFT),
                        TRANSFORM(namesRec,
                                            SELF.age := LEFT.age*2;
                                            SELF.ctr := LEFT.ctr + COUNTER ;
                                            SELF := LEFT))),
                NAMED('Form_4_Condition_Simple'));

OUTPUT(LOOP(namesTable2,
                        COUNTER <= MIN(namesTable2, age),
                        PROJECT(ROWS(LEFT),
                        TRANSFORM(namesRec,
                                            SELF.age := LEFT.age*2;
                                            SELF.ctr := LEFT.ctr + COUNTER ;
                                            SELF := LEFT))),
                NAMED('Form_4_Condition_ChildQ_Extern'));

//Form 5:

OUTPUT(LOOP(namesTable2,
                               LEFT.age < 100,
                              EXISTS(ROWS(LEFT)) and SUM(ROWS(LEFT), age) < 1000,
                               loopBody(ROWS(LEFT), COUNTER)));

