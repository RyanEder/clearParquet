# clearParquet
A minimal dependency fast parquet format writer and reader
If libzstd.a or libsnappy.a are found on the system, options to enable those compression algorithms will be enabled.

mkdir build
cd build
cmake ../ -DCMAKE_BUILD_TYPE=Release
make

# tests
writer
