# clearParquet
A minimal dependency fast parquet format writer and reader. Header only so it doesn't need a static lib built, but for ease of deployment for Linux environments, a clearParquet.a library is a target.
If libzstd.a and/or libsnappy.a are found on the system, options to enable those compression algorithms will be enabled.
For windows, zstd.lib and/or snappy.lib enable those compression algorithms. 

mkdir build
cd build
cmake ../ -DCMAKE_BUILD_TYPE=Release
make

# tests
writer

# Windows
clone into visual studio from git source.
Configure the two base variables for zstd and snappy if desired to point to the proper locations.
Build/ensure .dll's are present or accounted for in the exe.

