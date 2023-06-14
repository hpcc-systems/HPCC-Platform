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

//class=file
//nohthor        - does not execute anything in parallel
//nothor         - actions are run as separate child queries, and dependencies may be in separate sub graphs
//nokey

rtl := SERVICE
 unsigned4 msTick() :       eclrtl,library='eclrtl',entrypoint='rtlTick',volatile;
 unsigned4 sleep(unsigned4 _delay) : eclrtl,action,library='eclrtl',entrypoint='rtlSleep';
END;

//--- end of version configuration ---
import Std;

unsigned numRows := 21;
unsigned rowDelayMs := 10;
unsigned delayMs := (numRows - 1) * rowDelayMs;
unsigned seqDelayMs := (2 * numRows - 1) * rowDelayMs;
unsigned id1 := 0 : stored('id1');
unsigned id2 := 0 : stored('id2');
unsigned id3 := 0 : stored('id3');
unsigned id4 := 0 : stored('id4');

rec := { unsigned id, unsigned tick };
summaryRec := { unsigned minTick, unsigned maxTick };
mkRec(unsigned id) := TRANSFORM(rec, SELF.id := id; SELF.tick := rtl.msTick());

normalInput(unsigned id)   := DATASET(numRows, mkRec(id + COUNTER));
normalLatencyInput(unsigned id)   := DATASET(numRows, mkRec(id + COUNTER), HINT(hasrowlatency(true)));      // pretending to have latency
normalPrefetchInput(unsigned id)   := PREFETCH(normalInput(id));
slowInput(unsigned id)     := DATASET(numRows, mkRec(id + COUNTER + rtl.sleep(rowDelayMs)));
latencyInput(unsigned id)  := DATASET(numRows, mkRec(id + COUNTER + rtl.sleep(rowDelayMs)), HINT(hasrowlatency(true)));
prefetchInput(unsigned id) := PREFETCH(slowInput(id), numRows);

dependency(DATASET(rec) ds) := COUNT(NOFOLD(SORT(ds, id)));
normalDependency(unsigned seed) := dependency(normalInput(seed));
slowDependency(unsigned seed) := dependency(slowInput(seed));
latencyDependency(unsigned seed) := dependency(latencyInput(seed));
prefetchDependency(unsigned seed) := dependency(prefetchInput(seed));

//With child queries
childSlowInput(unsigned id) := DATASET(numRows, mkRec(id + COUNTER  + rtl.sleep(rowDelayMs) + normalDependency(COUNTER)));
lchildSlowInput(unsigned id) := DATASET(numRows, mkRec(id + COUNTER  + rtl.sleep(rowDelayMs) + latencyDependency(COUNTER)));

//With dependencies.  Try to code so that one dataset is run before any dependencies - but will not work with latency
initialTimings(unsigned id) := global(normalInput(id),few);
slowDepInput(unsigned id) := initialTimings(id) & normalInput(id + slowDependency(id));
latencyDepInput(unsigned id) := initialTimings(id) & normalInput(id + latencyDependency(id));
prefetchDepInput(unsigned id) := initialTimings(id) & normalInput(id + prefetchDependency(id));

summarise(ds) := FUNCTIONMACRO
  summary := TABLE(NOFOLD(ds), { unsigned minTick := MIN(GROUP, tick); unsigned maxTick := MAX(GROUP, tick) });
  elapsed := summary[1].maxTick - summary[1].minTick;
  RETURN elapsed;
ENDMACRO;

reportExpected(elapsed, expected) := FUNCTIONMACRO
    deltaMs := expected DIV 5; // Allow 20% variation
    isOk := IF (expected = 0, elapsed < 2 * rowDelayMs, elapsed BETWEEN expected - deltaMs and expected + deltaMs);
    RETURN IF (isOk, 'OK', 'Got ' + (string)elapsed + ' expected ' + (string)expected);
ENDMACRO;

checkExpected(ds, expected) := FUNCTIONMACRO
    elapsed := summarise(ds);
    RETURN reportExpected(elapsed, expected);
ENDMACRO;

sinkOutput(DATASET(rec) ds1, DATASET(rec) ds2, string version, integer expected) := FUNCTION
    prefix := IF(WORKUNIT != '', '~regress::prefetch::' + WORKUNIT + '::', '');
    o1 := output(ds1,,prefix + 'temp1',overwrite);
    o2 := output(ds2,,prefix + 'temp2',overwrite);
    results := DATASET(prefix + 'temp1', rec, THOR) + DATASET(prefix + 'temp2', rec, THOR);
    checkTime := output('Sink' + version + ': ' + checkExpected(results, expected));
    cleanup :=  IF(WORKUNIT != '', ordered(STD.File.DeleteLogicalFile(prefix+'temp1', true),STD.File.DeleteLogicalFile(prefix+'temp2', true)));
    RETURN SEQUENTIAL(PARALLEL(o1, o2), checkTime, cleanup);
END;

concatOutput(DATASET(rec) ds1, DATASET(rec) ds2, string version, integer expected) := FUNCTION
    results := (+)(ds1, ds2, ORDERED);
    checkTime := output('Concat' + version + ': ' + checkExpected(results, expected));
    RETURN checkTime;
END;

parConcatOutput(DATASET(rec) ds1, DATASET(rec) ds2, string version, integer expected) := FUNCTION
    results := ds1 + ds2;
    checkTime := output('ParConcat' + version + ': ' + checkExpected(results, expected));
    RETURN checkTime;
END;

joinOutput(DATASET(rec) ds1, DATASET(rec) ds2, string version, integer expected) := FUNCTION
    results := JOIN(ds1, ds2, LEFT.id = RIGHT.id, TRANSFORM(summaryRec, SELF.minTick := MIN(LEFT.tick, RIGHT.tick); SELF.maxTick := MAX(LEFT.tick, RIGHT.tick)));
    summary := TABLE(NOFOLD(results), { unsigned minTick := MIN(GROUP, minTick); unsigned maxTick := MAX(GROUP, maxTick) });
    elapsed := summary[1].maxTick - summary[1].minTick;
    checkTime := output('Join' + version + ': ' + reportExpected(elapsed, expected));
    RETURN checkTime;
END;

runTest(op, input1, input2, expected) := FUNCTIONMACRO
    in1 := #EXPAND(input1)(id1);
    in2 := #EXPAND(input2)(id2);
    version := '(' + input1 + ',' + input2 + ')';
        RETURN #EXPAND(op)(in1, in2, version, expected);
ENDMACRO;


runTests(input1, input2, expected) := MACRO
    runTest('sinkOutput', input1, input2, expected);
    runTest('concatOutput', input1, input2, expected);
ENDMACRO;

//Uncomment this to execute a single query
//single := runTest('concatOutput', 'latencyInput', 'latencyInput', seqDelayMs);

action := #IFDEFINED(single,
    sequential(
        runTest('sinkOutput', 'normalInput', 'normalInput', 0);
        runTest('sinkOutput', 'slowInput', 'slowInput', seqDelayMs);
        runTest('sinkOutput', 'latencyInput', 'latencyInput', delayMs);
        runTest('sinkOutput', 'prefetchInput', 'prefetchInput', seqDelayMs);
        runTest('concatOutput', 'slowInput', 'slowInput', seqDelayMs);
        runTest('concatOutput', 'latencyInput', 'latencyInput', seqDelayMs);
        runTest('concatOutput', 'prefetchInput', 'prefetchInput', delayMs);
        runTest('parConcatOutput', 'slowInput', 'slowInput', delayMs);
        runTest('parConcatOutput', 'latencyInput', 'latencyInput', delayMs);
        runTest('parConcatOutput', 'prefetchInput', 'prefetchInput', delayMs);
        runTest('joinOutput', 'slowInput', 'slowInput', seqDelayMs);
        runTest('joinOutput', 'latencyInput', 'latencyInput', seqDelayMs);
        runTest('joinOutput', 'prefetchInput', 'prefetchInput', delayMs);

        runTest('sinkOutput', 'slowDepInput', 'slowDepInput', seqDelayMs);
        runTest('sinkOutput', 'latencyDepInput', 'latencyDepInput', 0);                 // slow onstart should run in parallel - dependencies run before first timing can be gathered
        runTest('sinkOutput', 'prefetchDepInput', 'prefetchDepInput', seqDelayMs);      // urgent dependency does not imply slow
        runTest('concatOutput', 'slowDepInput', 'slowDepInput', seqDelayMs);
        runTest('concatOutput', 'latencyDepInput', 'latencyDepInput', 0);               // slow onstart should be done in parallel
        runTest('concatOutput', 'prefetchDepInput', 'prefetchDepInput', seqDelayMs);    // urgent != slow

        runTest('sinkOutput', 'slowDepInput', 'latencyDepInput', delayMs);              // one set of dependencies should be run 1st
        runTest('concatOutput', 'slowDepInput', 'latencyDepInput', delayMs);            // one set of dependencies should be run 1st
    );
);

action;
