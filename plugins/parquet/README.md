# Parquet Plugin for HPCC-Systems

The Parquet Plugin for HPCC-Systems is a powerful tool designed to facilitate the fast transfer of data stored in a columnar format to the ECL (Enterprise Control Language) data format. This plugin provides seamless integration between Parquet files and HPCC-Systems, enabling efficient data processing and analysis.

## Installation

The plugin uses vcpkg and can be installed by creating a separate build directory from the platform and running the following commands:
```
cd ./parquet-build
cmake -DPARQUETEMBED=ON ../HPCC-Platform
make -j4 package
sudo dpkg -i ./hpccsystems-plugin-parquetembed_<version>.deb
```

## Documentation

[Doxygen](https://www.doxygen.nl/index.html) can be used to create nice HTML documentation for the code. Call/caller graphs are also generated for functions if you have [dot](https://www.graphviz.org/download/) installed and available on your path.

Assuming `doxygen` is on your path, you can build the documentation via:
```
cd plugins/parquet
doxygen Doxyfile
```

## Features

The Parquet Plugin offers the following main functions:

### Regular Files

#### 1. Reading Parquet Files

The Read function allows ECL programmers to create an ECL dataset from both regular and partitioned Parquet files. It leverages the Apache Arrow interface for Parquet to efficiently stream data from ECL to the plugin, ensuring optimized data transfer.

```
dataset := Read(layout, '/source/directory/data.parquet');
```

#### 2. Writing Parquet Files

The Write function empowers ECL programmers to write ECL datasets to Parquet files. By leveraging the Parquet format's columnar storage capabilities, this function provides efficient compression and optimized storage for data.

```
Write(inDataset, '/output/directory/data.parquet');
```

### Partitioned Files (Tabular Datasets)

#### 1. Reading Partitioned Files

The Read Partition function extends the Read functionality by enabling ECL programmers to read from partitioned Parquet files. 

```
github_dataset := ReadPartition(layout, '/source/directory/partioned_dataset');
```

#### 2. Writing Partitioned Files

For partitioning parquet files all you need to do is run the Write function on Thor rather than hthor and each worker will create its own parquet file.
