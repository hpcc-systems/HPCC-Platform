import perform.system, perform.format, perform.files;

ds := files.diskPadded(false);

OUTPUT(COUNT(NOFOLD(ds)));
