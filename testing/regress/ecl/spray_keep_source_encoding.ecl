//class=spray
//nohthor
//noroxie

import Std.File AS FileServices;
import Std.Str AS StringServices;
import $.setup;

OriginalTextFilesIp :=  '.';
ClusterName := 'mythor';
ESPportIP := FileServices.GetEspURL() + '/FileSpray';

prefix := setup.Files(false, false).QueryFilePrefix;

dropzonePath := FileServices.GetDefaultDropZone() : STORED('dropzonePath');

unsigned VERBOSE := 0;

rec := RECORD
 DATA2 utf16;
END;

ds := DATASET([
  {x'fffe'}, {x'2700'}, {x'4900'}, {x'6400'}, {x'2700'}, {x'2c00'}, {x'2700'}, {x'4600'}, //  |..'.I.d.'.,.'.F.|
  {x'6900'}, {x'6500'}, {x'6c00'}, {x'6400'}, {x'3100'}, {x'2700'}, {x'2c00'}, {x'4600'}, //  |i.e.l.d.1.'.,.F.|
  {x'6900'}, {x'6500'}, {x'6c00'}, {x'6400'}, {x'3200'}, {x'2c00'}, {x'4600'}, {x'6900'}, //  |i.e.l.d.2.,.F.i.|
  {x'6500'}, {x'6c00'}, {x'6400'}, {x'3300'}, {x'2c00'}, {x'2700'}, {x'4600'}, {x'6900'}, //  |e.l.d.3.,.'.F.i.|
  {x'6500'}, {x'6c00'}, {x'6400'}, {x'3400'}, {x'2700'}, {x'0d00'}, {x'0a00'}, {x'2700'}, //  |e.l.d.4.'.....'.|
  {x'3000'}, {x'3000'}, {x'3000'}, {x'3000'}, {x'3000'}, {x'3000'}, {x'3000'}, {x'3000'}, //  |0.0.0.0.0.0.0.0.|
  {x'3000'}, {x'3000'}, {x'2700'}, {x'2c00'}, {x'2700'}, {x'6100'}, {x'6200'}, {x'2000'}, //  |0.0.'.,.'.a.b. .|
  {x'6400'}, {x'6500'}, {x'2700'}, {x'2c00'}, {x'3100'}, {x'3600'}, {x'3200'}, {x'2c00'}, //  |d.e.'.,.1.6.2.,.|
  {x'3700'}, {x'3200'}, {x'3200'}, {x'2c00'}, {x'2700'}, {x'6100'}, {x'2000'}, {x'6300'}, //  |7.2.2.,.'.a. .c.|
  {x'6400'}, {x'6500'}, {x'6600'}, {x'6700'}, {x'6800'}, {x'6900'}, {x'2000'}, {x'6b00'}, //  |d.e.f.g.h.i. .k.|
  {x'2000'}, {x'2000'}, {x'6e00'}, {x'6f00'}, {x'7000'}, {x'7100'}, {x'7200'}, {x'7300'}, //  | . .n.o.p.q.r.s.|
  {x'7400'}, {x'7500'}, {x'7600'}, {x'7700'}, {x'7800'}, {x'7900'}, {x'7a00'}, {x'3000'}, //  |t.u.v.w.x.y.z.0.|
  {x'3100'}, {x'3200'}, {x'2000'}, {x'2000'}, {x'2000'}, {x'3600'}, {x'3700'}, {x'3800'}, //  |1.2. . . .6.7.8.|
  {x'2000'}, {x'2700'}, {x'0d00'}, {x'0a00'}                                               // | .'.....|
               ], rec);

// To avoid the problem with an upper case letter 'W' (from the WORKUNIT) in the logical file name
SourceFileName := StringServices.ToLowerCase(WORKUNIT) + '-data-utf-16le.csv';
SourceFile := dropzonePath + '/' + SourceFileName;

rec2 := RECORD
    UTF8 field1;
    UTF8 field2;
    UTF8 field3;
    UTF8 field4;
    UTF8 field5;
END;

rec3 := RECORD
 DATA utf16;
END;



DestFile1 := prefix + 'Data-utf-8.csv';
ds1 := DATASET(DestFile1, rec2, csv);

DestFile2 := prefix + 'Data-utf-16le.csv';
ds2 := DATASET(DestFile2, rec3, csv(TERMINATOR(x'0d0a'),SEPARATOR(x'00')));

DestFile3 := prefix + 'Data-utf-16le3.csv';
ds3 := DATASET(DestFile3, rec3, csv(TERMINATOR(x'0d0a'),SEPARATOR(x'00')));

SEQUENTIAL (
    #if (VERBOSE = 1)
	output(OriginalTextFilesIp, NAMED('OriginalTextFilesIp'));
	output(SourceFileName, NAMED('SourceFileName'));
	output(SourceFile, NAMED('SourceFile'));
	output(Destfile1, NAMED('Destfile1'));
	output(Destfile2, NAMED('Destfile2'));
	output(Destfile3, NAMED('Destfile3'));
    #end

    // It would be nice to convert DropZone path (returned by GetDefaultDropZone()) to escaped string
    OUTPUT(ds, , '~file::localhost::var::lib::^H^P^C^C^Systems::mydropzone::' + SourceFileName, OVERWRITE);

    // Spray with force to convert target file to UTF-8 encoding
    FileServices.SprayVariable(OriginalTextFilesIp,
                        SourceFile,
                        destinationGroup := ClusterName,
                        destinationLogicalName := DestFile1,
                        espServerIpPort := ESPportIP,
                        ALLOWOVERWRITE := true,
                        ENCODING := 'utf16le',
                        KEEPSOURCEENCODING:=false);

    output(FileServices.GetLogicalFileAttribute(DestFile1, 'format'), named('format1'));
    output(FileServices.GetLogicalFileAttribute(DestFile1, 'kind'), named('kind1'));
	output(ds1, NAMED('Ds1'));


    // Spray with explicit keep original (UTF-16LE) encoding
    FileServices.SprayVariable(OriginalTextFilesIp,
                        SourceFile,
                        destinationGroup := ClusterName,
                        destinationLogicalName := DestFile2,
                        espServerIpPort := ESPportIP,
                        ALLOWOVERWRITE := true,
                        ENCODING := 'utf16le',
                        KEEPSOURCEENCODING:=true);

    output(FileServices.GetLogicalFileAttribute(DestFile2, 'format'), named('format2'));
    output(FileServices.GetLogicalFileAttribute(DestFile2, 'kind'), named('kind2'));
	output(ds2, NAMED('Ds2'));
	
	
    // Spray default keep original (UTF-16LE) encoding
    FileServices.SprayVariable(OriginalTextFilesIp,
                        SourceFile,
                        destinationGroup := ClusterName,
                        destinationLogicalName := DestFile3,
                        espServerIpPort := ESPportIP,
                        ALLOWOVERWRITE := true,
                        ENCODING := 'utf16le'
			);

    output(FileServices.GetLogicalFileAttribute(DestFile3, 'format'), named('format3'));
    output(FileServices.GetLogicalFileAttribute(DestFile3, 'kind'), named('kind3'));
	output(ds3, NAMED('Ds3'));

    // Clean-up
    FileServices.DeleteExternalFile('.', SourceFile),

    FileServices.DeleteLogicalFile(DestFile1),
    FileServices.DeleteLogicalFile(DestFile2),
    FileServices.DeleteLogicalFile(DestFile3),
);
