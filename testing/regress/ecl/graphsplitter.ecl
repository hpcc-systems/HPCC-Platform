/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2020 HPCC Systems®.

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


idRec := { unsigned id; };
idCntRec := { unsigned id; unsigned cnt };

values := DATASET(255, transform(idRec, SELF.id := COUNTER));

gatherBits(unsigned x) := FUNCTION
    dsRaw := DATASET(100, TRANSFORM(idRec, SELF.id := COUNTER + x));
	ds := NOCOMBINE(SORT(dsRaw, id));
    f1 := IF((x & 1) != 0, ds);
    f2 := IF((x & 2) != 0, ds);
    f4 := IF((x & 4) != 0, ds);
    f8 := IF((x & 8) != 0, ds);
    f16 := IF((x & 16) != 0, ds);
    f32 := IF((x & 32) != 0, ds);
    f64 := IF((x & 64) != 0, ds);
    f128 := IF((x & 128) != 0, ds);

    f := f1 & f2 & f4 & f8 & f16 & f32 & f64 & f128;
	RETURN f;
END;

numberOfBitsBoring(unsigned x) := FUNCTION
	temp1 := x - ((x >> 1) & 0x55555555U);
    temp2 := (temp1 & 0x33333333) + ((temp1 >> 2) & 0x33333333);
    RETURN (((temp2 + (temp2 >> 4)) & 0x0F0F0F0F) * 0x01010101) >> 24;
END;

numberOfBits1(unsigned x) := FUNCTION
    f := gatherBits(x);
    c := COUNT(NOCOMBINE(f))/ 100;
	RETURN c;
END;

numberOfBits2(unsigned x) := FUNCTION
  	f := GRAPH(DATASET([], idRec), NOFOLD(1), gatherBits(x));
	RETURN COUNT(NOCOMBINE(f))/ 100;
END;

//Ensure the bits function is executed within the graph
numberOfBits3(unsigned x) := FUNCTION
  	f := GRAPH(DATASET([], idRec), NOFOLD(1), gatherBits(x + (COUNTER * NOFOLD(0))));
	RETURN COUNT(NOCOMBINE(f))/ 100;
END;

//Ensure the bits function is executed within the graph
numberOfBits4(unsigned x) := FUNCTION
  	f := GRAPH(DATASET([], idRec), NOFOLD(1), gatherBits(x + (COUNTER * NOFOLD(0))), parallel);
	RETURN COUNT(NOCOMBINE(f))/ 100;
END;


p1 := PROJECT(values, transform(idCntRec, SELF.id := LEFT.id; SELF.cnt := numberOfBits1(LEFT.id) - numberOfBitsBoring(LEFT.id)));
o1 := output(NOFOLD(p1)(cnt != 0));

p2 := PROJECT(values, transform(idCntRec, SELF.id := LEFT.id; SELF.cnt := numberOfBits2(LEFT.id) - numberOfBitsBoring(LEFT.id)));
o2 := output(NOFOLD(p2)(cnt != 0));

p3 := PROJECT(values, transform(idCntRec, SELF.id := LEFT.id; SELF.cnt := numberOfBits3(LEFT.id) - numberOfBitsBoring(LEFT.id)));
o3 := output(NOFOLD(p3)(cnt != 0));

p4 := PROJECT(values, transform(idCntRec, SELF.id := LEFT.id; SELF.cnt := numberOfBits4(LEFT.id) - numberOfBitsBoring(LEFT.id)));
o4 := output(NOFOLD(p4)(cnt != 0));

sequential(o1, o2, o3, o4);
