import perform.system;
import perform.format;
import perform.files;

ds := files.generateSimple();

cnt := COUNT(NOFOLD(ds));

OUTPUT(cnt);
