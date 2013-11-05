import perform.system, perform.format, perform.files;

ds := files.diskSimple(false);

OUTPUT(COUNT(NOFOLD(ds)));
