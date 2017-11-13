import $.setup;
prefix := setup.Files(false, false).FilePrefix;

//Sample data
rec := record
	string s1;
	string s2;
end;
ds0 := dataset([{'xxx','yyy'},{'xx,x','y,yy'},{'zzz','z'}],rec);
//Output dataset
ds0;

//Save dataset to file with heading option
output(ds0,,prefix+'jas_testout',csv(heading,SEPARATOR(','),quote('"')),overwrite,expire);