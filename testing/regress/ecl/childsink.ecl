/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2021 HPCC Systems®.

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

// Testing various forms of child query sinks

//version childSinkOption='sequential'
//version childSinkOption='parallel'
//version childSinkOption='parallelPersistent'

import ^ as root;
childSinkMode := #IFDEFINED(root.childSinkOption, 'parallel');

Simple := FALSE;
Full := TRUE;

Layout := RECORD
  UNSIGNED8 UID;
	UNSIGNED4 F1;
	UNSIGNED4 F2;
	UNSIGNED4 F3;
	UNSIGNED4 F4;
	UNSIGNED4 F5;
	UNSIGNED4 F6;
	UNSIGNED4 F7;
	UNSIGNED4 F8;
	UNSIGNED4 F9;
	UNSIGNED4 F10;
	UNSIGNED4 F11;
	UNSIGNED4 F12;
	UNSIGNED4 F13;
	UNSIGNED4 F14;
	UNSIGNED4 F15;
	UNSIGNED4 F16;
	UNSIGNED4 F17;
	UNSIGNED4 F18;
	UNSIGNED4 F19;
	UNSIGNED4 F20;
	UNSIGNED4 F21;
	UNSIGNED4 F22;
	UNSIGNED4 F23;
	UNSIGNED4 F24;
	UNSIGNED4 F25;
END;

DUP_UIDS := 4;
DUP_Values := 1;
Layout BuildData(Layout r, UNSIGNED c) := TRANSFORM
  SELF.UID := c DIV DUP_UIDS;
  SELF.F1 := HASH32(c DIV DUP_Values + 1);
  SELF.F2 := HASH32(c DIV DUP_Values + 2);
  SELF.F3 := HASH32(c DIV DUP_Values + 3);
  SELF.F4 := HASH32(c DIV DUP_Values + 4);
  SELF.F5 := HASH32(c DIV DUP_Values + 5);
  SELF.F6 := HASH32(c DIV DUP_Values + 6);
  SELF.F7 := HASH32(c DIV DUP_Values + 7);
  SELF.F8 := HASH32(c DIV DUP_Values + 8);
  SELF.F9 := HASH32(c DIV DUP_Values + 9);
  SELF.F10 := HASH32(c DIV DUP_Values + 10);
  SELF.F11 := HASH32(c DIV DUP_Values + 11);
  SELF.F12 := HASH32(c DIV DUP_Values + 12);
  SELF.F13 := HASH32(c DIV DUP_Values + 13);
  SELF.F14 := HASH32(c DIV DUP_Values + 14);
  SELF.F15 := HASH32(c DIV DUP_Values + 15);
  SELF.F16 := HASH32(c DIV DUP_Values + 16);
  SELF.F17 := HASH32(c DIV DUP_Values + 17);
  SELF.F18 := HASH32(c DIV DUP_Values + 18);
  SELF.F19 := HASH32(c DIV DUP_Values + 19);
  SELF.F20 := HASH32(c DIV DUP_Values + 20);
  SELF.F21 := HASH32(c DIV DUP_Values + 21);
  SELF.F22 := HASH32(c DIV DUP_Values + 22);
  SELF.F23 := HASH32(c DIV DUP_Values + 23);
  SELF.F24 := HASH32(c DIV DUP_Values + 24);
  SELF.F25 := HASH32(c DIV DUP_Values + 25);
END;

Seed := DATASET([{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0}], Layout);
Source := NORMALIZE(Seed, 20000, BuildData(LEFT, COUNTER));
Layout SimpleRollup(Layout r, DATASET(Layout) rs) := TRANSFORM
  SELF := r;
END;
SimpleResult := ROLLUP(GROUP(Source, UID), GROUP, SimpleRollup(LEFT, ROWS(LEFT)));
IF(Simple, OUTPUT(SimpleResult, , '~tmp::kel::rollupslowdown::simple', OVERWRITE, EXPIRE(1)));

Layout FullRollup(Layout r, DATASET(Layout) rs) := TRANSFORM
  SELF.F1 := TOPN(TABLE(rs, {F1,UNSIGNED C:=COUNT(GROUP)}, F1, FEW), 1, -C)[1].F1;  
  SELF.F2 := TOPN(TABLE(rs, {F2,UNSIGNED C:=COUNT(GROUP)}, F2, FEW), 1, -C)[1].F2;  
  SELF.F3 := TOPN(TABLE(rs, {F3,UNSIGNED C:=COUNT(GROUP)}, F3, FEW), 1, -C)[1].F3;  
/*
  SELF.F4 := TOPN(TABLE(rs, {F4,UNSIGNED C:=COUNT(GROUP)}, F4, FEW), 1, -C)[1].F4;  
  SELF.F5 := TOPN(TABLE(rs, {F5,UNSIGNED C:=COUNT(GROUP)}, F5, FEW), 1, -C)[1].F5;  
  SELF.F6 := TOPN(TABLE(rs, {F6,UNSIGNED C:=COUNT(GROUP)}, F6, FEW), 1, -C)[1].F6;  
  SELF.F7 := TOPN(TABLE(rs, {F7,UNSIGNED C:=COUNT(GROUP)}, F7, FEW), 1, -C)[1].F7;  
  SELF.F8 := TOPN(TABLE(rs, {F8,UNSIGNED C:=COUNT(GROUP)}, F8, FEW), 1, -C)[1].F8;  
  SELF.F9 := TOPN(TABLE(rs, {F9,UNSIGNED C:=COUNT(GROUP)}, F9, FEW), 1, -C)[1].F9;  
  SELF.F10 := TOPN(TABLE(rs, {F10,UNSIGNED C:=COUNT(GROUP)}, F10, FEW), 1, -C)[1].F10;  
  SELF.F11 := TOPN(TABLE(rs, {F11,UNSIGNED C:=COUNT(GROUP)}, F11, FEW), 1, -C)[1].F11;  
  SELF.F12 := TOPN(TABLE(rs, {F12,UNSIGNED C:=COUNT(GROUP)}, F12, FEW), 1, -C)[1].F12;  
  SELF.F13 := TOPN(TABLE(rs, {F13,UNSIGNED C:=COUNT(GROUP)}, F13, FEW), 1, -C)[1].F13;  
  SELF.F14 := TOPN(TABLE(rs, {F14,UNSIGNED C:=COUNT(GROUP)}, F14, FEW), 1, -C)[1].F14;  
  SELF.F15 := TOPN(TABLE(rs, {F15,UNSIGNED C:=COUNT(GROUP)}, F15, FEW), 1, -C)[1].F15;  
  SELF.F16 := TOPN(TABLE(rs, {F16,UNSIGNED C:=COUNT(GROUP)}, F16, FEW), 1, -C)[1].F16;  
  SELF.F17 := TOPN(TABLE(rs, {F17,UNSIGNED C:=COUNT(GROUP)}, F17, FEW), 1, -C)[1].F17;  
  SELF.F18 := TOPN(TABLE(rs, {F18,UNSIGNED C:=COUNT(GROUP)}, F18, FEW), 1, -C)[1].F18;  
  SELF.F19 := TOPN(TABLE(rs, {F19,UNSIGNED C:=COUNT(GROUP)}, F19, FEW), 1, -C)[1].F19;  
  SELF.F20 := TOPN(TABLE(rs, {F20,UNSIGNED C:=COUNT(GROUP)}, F20, FEW), 1, -C)[1].F20;  
  SELF.F21 := TOPN(TABLE(rs, {F21,UNSIGNED C:=COUNT(GROUP)}, F21, FEW), 1, -C)[1].F21;  
  SELF.F22 := TOPN(TABLE(rs, {F22,UNSIGNED C:=COUNT(GROUP)}, F22, FEW), 1, -C)[1].F22;  
  SELF.F23 := TOPN(TABLE(rs, {F23,UNSIGNED C:=COUNT(GROUP)}, F23, FEW), 1, -C)[1].F23;  
  SELF.F24 := TOPN(TABLE(rs, {F24,UNSIGNED C:=COUNT(GROUP)}, F24, FEW), 1, -C)[1].F24;  
  SELF.F25 := TOPN(TABLE(rs, {F25,UNSIGNED C:=COUNT(GROUP)}, F25, FEW), 1, -C)[1].F25;  
*/
  SELF := r;
END;
FullResult := PULL(ROLLUP(GROUP(Source, UID), GROUP, FullRollup(LEFT, ROWS(LEFT)), HINT(sinkMode(childSinkMode))));
OUTPUT(CHOOSEN(FullResult, 1));
