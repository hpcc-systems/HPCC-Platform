/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
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

