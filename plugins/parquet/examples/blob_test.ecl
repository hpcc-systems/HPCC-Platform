IMPORT STD;
IMPORT PARQUET;

imageRecord := RECORD
    STRING filename;
    DATA image;
    UNSIGNED8 RecPos{virtual(fileposition)};
END;

#IF(0)
in_image_data := DATASET('~parquet::image', imageRecord, FLAT);
OUTPUT(in_image_data, NAMED('IN_IMAGE_DATA'));
PARQUET.Write(in_image_data, '/datadrive/dev/test_data/test_image.parquet');

#END;

#IF(1)
out_image_data := Read({DATA image}, '/datadrive/dev/test_data/test_image.parquet');
OUTPUT(out_image_data, NAMED('OUT_IMAGE_DATA'));
#END