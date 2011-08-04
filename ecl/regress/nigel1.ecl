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

// TEST1

tFile :=
    record
        String5 K;
        String5 F;
    end;

tFile JoinTransform(tFile L, tFile R) :=
    transform
        self.K := L.K;
        self.F := R.F;
    end;


File1           := DATASET ('testjoin01', tFile, flat);
File2           := DATASET ('testjoin02', tFile, flat);
Join1           := JOIN(File1,File2,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join2           := JOIN(File2,Join1,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join3           := JOIN(Join1,Join2,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join4           := JOIN(Join2,Join3,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join5           := JOIN(Join3,Join4,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join6           := JOIN(Join4,Join5,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join7           := JOIN(Join5,Join6,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join8           := JOIN(Join6,Join7,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join9           := JOIN(Join7,Join8,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join10          := JOIN(Join8,Join9,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join11          := JOIN(Join9,Join10,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join12          := JOIN(Join10,Join11,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join13          := JOIN(Join11,Join12,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join14          := JOIN(Join12,Join13,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join15          := JOIN(Join13,Join14,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join16          := JOIN(Join14,Join15,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join17          := JOIN(Join15,Join16,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join18          := JOIN(Join16,Join17,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join19          := JOIN(Join17,Join18,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join20          := JOIN(Join18,Join19,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join21          := JOIN(Join19,Join20,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join22          := JOIN(Join20,Join21,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join23          := JOIN(Join21,Join22,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join24          := JOIN(Join22,Join23,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join25          := JOIN(Join23,Join24,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join26          := JOIN(Join24,Join25,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join27          := JOIN(Join25,Join26,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join28          := JOIN(Join26,Join27,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join29          := JOIN(Join27,Join28,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join30          := JOIN(Join28,Join29,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join31          := JOIN(Join29,Join30,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join32          := JOIN(Join30,Join31,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join33          := JOIN(Join31,Join32,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join34          := JOIN(Join32,Join33,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join35          := JOIN(Join33,Join34,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join36          := JOIN(Join34,Join35,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join37          := JOIN(Join35,Join36,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join38          := JOIN(Join36,Join37,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join39          := JOIN(Join37,Join38,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join40          := JOIN(Join38,Join39,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join41          := JOIN(Join39,Join40,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join42          := JOIN(Join40,Join41,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join43          := JOIN(Join41,Join42,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join44          := JOIN(Join42,Join43,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join45          := JOIN(Join43,Join44,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join46          := JOIN(Join44,Join45,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join47          := JOIN(Join45,Join46,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join48          := JOIN(Join46,Join47,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join49          := JOIN(Join47,Join48,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));
Join50          := JOIN(Join48,Join49,LEFT.K=RIGHT.K,JoinTransform(LEFT,RIGHT));

Count(Join50);

