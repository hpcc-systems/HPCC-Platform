import perform.system, perform.format, perform.files;

ds := files.xmlSimple(false);

OUTPUT(COUNT(NOFOLD(ds)));
