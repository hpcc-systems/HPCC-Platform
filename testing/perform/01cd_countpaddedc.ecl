import perform.system, perform.format, perform.files;

ds := files.diskPadded(true);

OUTPUT(COUNT(NOFOLD(ds)));
