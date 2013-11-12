import perform.create;

LOADXML('<xml/>');

dsAll := create.orderedAppend(4);

cnt := COUNT(NOFOLD(dsAll));

OUTPUT(cnt);
