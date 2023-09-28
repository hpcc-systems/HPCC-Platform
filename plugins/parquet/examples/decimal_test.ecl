IMPORT STD;
IMPORT PARQUET;


layout := RECORD
    DECIMAL5_2 height;
END; 

decimal_data := DATASET([{152.25}, {125.56}], layout);

#IF(1)
Write(decimal_data, '/datadrive/dev/test_data/decimal.parquet');
#END

#IF(1)
Read(layout, '/datadrive/dev/test_data/decimal.parquet');
#END
