import perform.system, perform.format, perform.files;

ds := files.csvSimple(false);

OUTPUT(COUNT(NOFOLD(ds)));
