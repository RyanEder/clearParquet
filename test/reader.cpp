#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "ParquetFileReader.hpp"

int main(int argc, char **argv) {
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");

    auto reader = clearParquet::ParquetFileReader::Open(infile);
    auto schema = reader->schema();

    for (const auto& batch : *reader){
        // specific col callback here.
        for (const auto& val : batch->_strCols->Get()) {
            std::cout << val << std::endl;
        }
    }

    return 0;
}
