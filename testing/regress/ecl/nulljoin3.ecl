/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems.

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




rec2 := RECORD,maxlength(64)
        INTEGER4 pad1;
        STRING key;
        INTEGER4 pad2;
       END;
       
       
rec1 := RECORD
        INTEGER8 pad1;
        STRING10 key;
        INTEGER8 pad2;
       END;
       
       
       
DS1 := DATASET([ {1,'A',1},{2,'BB',1},{3,'C',1},{4,'DDD',1},{5,'E',1},{6,'FFFF',1},{7,'G',1},{8,'HHHHH',1},{9,'I',1},{10,'JJJJJJ',1},
               {11,'L',1},{12,'LLLLLLL',1},{13,'M',1},{14,'NNNNNNNN',1},{15,'O',1},{16,'PPPPPPPPP',1},{17,'Q',1},{18,'RRRRRRRRR',1},{19,'S',1},{20,'TTTTTTTT',1},
               {21,'U',1},{22,'VVVVVVV',1},{23,'WWWWW',1},{24,'X',1},{25,'YYY',1},{26,'Z',1}
             ], rec1);
             

DS2 := DATASET([ {1,'A',2},{2,'BB',2},{3,'C',2},{4,'DDD',2},{5,'E',2},{6,'FFFF',2},{7,'G',2},{8,'HHHHH',2},{9,'I',2},{10,'JJJJJJ',2},
               {11,'L',2},{12,'LLLLLLL',2},{13,'M',2},{14,'NNNNNNNN',2},{15,'O',2},{16,'PPPPPPPPP',2},{17,'Q',2},{18,'RRRRRRRRR',2},{19,'S',2},{20,'TTTTTTTT',2},
               {21,'U',2},{22,'VVVVVVV',2},{23,'WWWWW',2},{24,'X',2},{25,'YYY',2},{26,'Z',2}
             ], rec2);
             
             

HD1 := DISTRIBUTE(DS1,pad1+HASH(key));
HD2 := DISTRIBUTE(DS2,HASH(key)-pad1);

rec3 := RECORD
        STRING10 key;
        INTEGER4 sum;
    END;
        
rec3 T1(rec1 l,rec2 r) := TRANSFORM
SELF.key := l.key;
SELF.sum := l.pad1+l.pad2+r.pad1+r.pad2;
END;        

rec3 T2(rec2 l,rec1 r) := TRANSFORM
SELF.key := l.key;
SELF.sum := l.pad1+l.pad2+r.pad1+r.pad2;
END;        

OUTPUT(JOIN(HD1,HD2,LEFT.key=RIGHT.KEY,T1(LEFT,RIGHT)));
OUTPUT(JOIN(HD1(pad2=3),HD2,LEFT.key=RIGHT.KEY,T1(LEFT,RIGHT)));
OUTPUT(JOIN(HD2(pad2=3),HD1,LEFT.key=RIGHT.KEY,T2(LEFT,RIGHT)));
OUTPUT(JOIN(HD1,HD2,LEFT.key=RIGHT.KEY,T1(LEFT,RIGHT),FULL ONLY));
OUTPUT(JOIN(HD1(pad2=3),HD2,LEFT.key=RIGHT.KEY,T1(LEFT,RIGHT),FULL ONLY));
OUTPUT(JOIN(HD2(pad2=3),HD1,LEFT.key=RIGHT.KEY,T2(LEFT,RIGHT),FULL ONLY));


             
       
       
       
        