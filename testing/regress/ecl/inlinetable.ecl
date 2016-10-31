
/*** Shared definitions ***/
FID := ENUM(f1, f2, none);

childRec := { real value, unsigned id };
stateRec := { FID fieldId, embedded dataset(childRec) ids };

inputRec := { real f1, real f2, unsigned expected; };

unsigned nextState(inputRec le, unsigned4 stateId, linkcounted dataset(stateRec) mapping) := FUNCTION

    curState := mapping[stateId];
    testValue := (CASE(curState.fieldId,
        FID.f1=>le.f1,
        FID.f2=>le.f2,
        0));

    RETURN IF(stateId <= count(mapping), curState.ids(testValue < value)[NOBOUNDCHECK 1].id, stateId);
END;

/* Demo example */

processTree3(inputRec le, linkcounted dataset(stateRec) mapping) := FUNCTION
    step1 := nextState(le, 1, mapping);
    step2 := nextState(le, step1, mapping);
    step3 := nextState(le, step2, mapping);
    RETURN step3;
END;

processTree(le, mapping, iters) := FUNCTIONMACRO
    step1 := nextState(le, 1, mapping);
    #DECLARE (count)
    #DECLARE (next)
    #DECLARE (countvar)
    #DECLARE (nextvar)
    #SET (count, 1);
    #LOOP
        #IF (%count%>=iters)
            #BREAK
        #END
        #SET (next, %count%+1);
        #SET (countVar, 'step' + (string)%count%)
        #SET (nextVar, 'step' + (string)%next%)
    %nextVar% := nextState(le, %countVar%, mapping);
        #SET (count, %next%);
    #END
    #SET (countVar, 'step' + (string)%count%)
    RETURN %countVar%;
ENDMACRO;

//Function which expands out up to 11 levels of tree
processTree11(inputRec le, linkcounted dataset(stateRec) mapping) := processTree(le, mapping, 11);

/**** Definition for a single tree ****/

/*
f1 < 5
  f2 < 3
    f2 < 2
      100
      101
    f1 < 1
      102
      103
  f2 < 2
    f1 < 8.5
      104
      105
    106
*/

myStates := DATASET([
    { FID.f1, [{ 5.0, 2 }, { 9999999.0, 3}]},
    { FID.f2, [{3.0, 4}, { 9999999.0, 5}]},
    { FID.f2, [{2.0, 6}, { 9999999.0, 106}]}, // truncated tree
    { FID.f2, [{2.0, 100}, { 9999999.0, 101}]},
    { FID.f1, [{1.0, 102}, { 9999999.0, 103}]},
    { FID.f1, [{8.5, 104}, { 9999999.0, 105}]}
    ], stateRec);

set of real scoreMapping := [ 1.2, 12.6, -3.5, 9, 9.2, 12.6, 19.3 ];

//Slight variation on above
myStates2 := DATASET([
    { FID.f1, [{5.0, 2}, { 9999999.0, 3}]},
    { FID.f2, [{3.0, 4}, { 9999999.0, 5}]},
    { FID.f2, [{2.0, 106}, { 9999999.0, 6}]}, // truncated tree
    { FID.f2, [{2.0, 100}, { 9999999.0, 101}]},
    { FID.f1, [{1.0, 103}, { 9999999.0, 102}]},
    { FID.f1, [{8.5, 104}, { 9999999.0, 105}]}
    ], stateRec);

/* Definition of the input dataset */

myDataset := DATASET([
    {3.4, 1.6, 100},
    {3.4, 2.6, 101},
    {0.4, 3.6, 102},
    {3.4, 3.6, 103},
    {6.4, 1.6, 104},
    {8.7, 1.6, 105},
    {8.9, 3.7, 106}
    ], inputRec);

outputRec := RECORD(inputRec)
    unsigned calculated;
    unsigned calculated2;
    real score;
END;

outputRec t(inputRec l) := TRANSFORM
    whichOutput := processTree11(l, myStates);
    SELF.calculated := whichOutput;
    SELF.calculated2 := processTree11(l, myStates2);    // result of the second tree
    SELF.score := scoreMapping[whichOutput-99]; // map a tree index to a score
    SELF := l;
END;

p := PROJECT(myDataset, t(LEFT));

OUTPUT(p);
