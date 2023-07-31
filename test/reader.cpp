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
        for (const auto& val : batch->_strCols->Get()) {
            std::cout << val << std::endl;
        }
    }
    //std::shared_ptr<clearParquet::RecordBatch> batch;
    //while(true) {
    //    reader->ReadNext(&batch);
    //    if (batch == nullptr) {
    //        break;
    //    }
    //    for (int i = 0 ; i < batch->num_columns(); ++i) {
    //        std::cout << "Column " << i << ": " << batch->column(i)->ToString() << std::endl;
    //    }
    //}
    return 0;
}
