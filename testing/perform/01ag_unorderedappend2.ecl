import perform.create;

LOADXML('<xml/>');

dsAll := create.orderedAppend(2);

cnt := COUNT(NOFOLD(dsAll));

OUTPUT(cnt);
