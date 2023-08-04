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
        batch->PrintBatch();
    }

    for (const auto& batch : *reader){
        for (uint64_t i = 0; i < batch->NumColumns(); ++i) {
            std::cout << "Column " << i << ": " << batch->Column(i)->ToString() << std::endl;
            for (uint64_t j = 0; j < batch->Column(i)->Size(); ++j) {
                auto val = batch->Column(i)->Value(j);
                auto type = batch->Column(i)->GetType();
                if (type == clearParquet::Type::INT32) {
                    std::cout << std::any_cast<uint32_t>(val) << std::endl;
                } else if (type == clearParquet::Type::INT64) {
                    std::cout << std::any_cast<uint64_t>(val) << std::endl;
                } else if (type == clearParquet::Type::BOOLEAN) {
                    std::cout << std::any_cast<bool>(val) << std::endl;
                } else if (type == clearParquet::Type::FLOAT) {
                    std::cout << std::any_cast<float>(val) << std::endl;
                } else if (type == clearParquet::Type::DOUBLE) {
                    std::cout << std::any_cast<double>(val) << std::endl;
                } else if (type == clearParquet::Type::BYTE_ARRAY) {
                    std::cout << std::any_cast<std::string>(val) << std::endl;
                }
            }
        }
    }


    return 0;
}
