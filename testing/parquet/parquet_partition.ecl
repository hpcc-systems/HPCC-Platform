//class=ParquetRegression

IMPORT Std;
IMPORT Parquet;

hiveLayout := RECORD
    INTEGER ID {XPATH('ID')};
    STRING  NAME {XPATH('NAME')};
    INTEGER AGE {XPATH('AGE')};
END;

dirLayout := RECORD
    INTEGER ID {XPATH('ID')};
    STRING  NAME {XPATH('NAME')};
    INTEGER AGE {XPATH('AGE')};
    STRING  COUNTRY {XPATH('COUNTRY')};
END;

hiveFilePath1 := '/var/lib/HPCCSystems/mydropzone/hive1.parquet';
hiveFilePath2 := '/var/lib/HPCCSystems/mydropzone/hive2.parquet';
dirFilePath1 := '/var/lib/HPCCSystems/mydropzone/directory1.parquet';
dirFilePath2 := '/var/lib/HPCCSystems/mydropzone/directory2.parquet';

hiveData1 := ParquetIO.Read(hiveLayout, hiveFilePath1);
hiveData2 := ParquetIO.Read(hiveLayout, hiveFilePath2);
dirData1 := ParquetIO.Read(dirLayout, dirFilePath1);
dirData2 := ParquetIO.Read(dirLayout, dirFilePath2);

OUTPUT(hiveData1, NAMED('HiveData1'));
OUTPUT(hiveData2, NAMED('HiveData2'));
OUTPUT(dirData1, NAMED('DirData1'));
OUTPUT(dirData2, NAMED('DirData2'));

PartitionedHiveData := DISTRIBUTE(hiveData1, ID);

ParquetIO.HivePartition.Write(PartitionedHiveData, 100000, '/var/lib/HPCCSystems/mydropzone/partitioned_hive_data1/hive_partitioned5_new.parquet', TRUE, 'ID');

HivePartitionResult := IF(COUNT(PartitionedHiveData) > 0, 'Pass: Hive Partitioning', 'Fail: Hive Partitioning');
OUTPUT(HivePartitionResult, NAMED('HivePartitioningResult'));

PartitionedDirData := DISTRIBUTE(dirData1, ID);

ParquetIO.DirectoryPartition.Write(PartitionedDirData, 100000, '/var/lib/HPCCSystems/mydropzone/partitioned_dir_data1/dir_partitioned5_new.parquet', TRUE, 'ID');

DirectoryPartitionResult := IF(COUNT(PartitionedDirData) > 0, 'Pass: Directory Partitioning', 'Fail: Directory Partitioning');
OUTPUT(DirectoryPartitionResult, NAMED('DirectoryPartitioningResult'));