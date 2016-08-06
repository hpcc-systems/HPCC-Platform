import perform.create;

LOADXML('<xml/>');

dsAll := create.orderedAppend(32);

cnt := COUNT(NOFOLD(dsAll));

OUTPUT(cnt);
