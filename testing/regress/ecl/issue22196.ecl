import Std.System.Workunit as Wu;

f(STRING i) := FUNCTION
    chk := ASSERT(i != 'A','Letter \'A\' not allowed',FAIL);
    RETURN WHEN(OUTPUT(i),chk);
END : FAILURE(OUTPUT(FAILMESSAGE));

isFirstRun := NOT EXISTS(Wu.WorkunitMessages(WORKUNIT));
arg := IF(isFirstRun, 'A', 'B');

f(arg) : recovery(output('Recover'));
