import perform.system, perform.format, perform.files;

ds := files.diskSimple(true);

OUTPUT(COUNT(NOFOLD(ds)));
