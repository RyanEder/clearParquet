#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

//#include "arrow/io/file.h"
//#include "parquet/stream_writer.h"
//#include "parquet/arrow/writer.h"
#include "ParquetColumnChunk.hpp"
#include "ParquetPageHeader.hpp"
#include "ParquetStreamWriter.hpp"

int main() {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");

    clearParquet::NodeVector columnNames{};

    columnNames.push_back(
        clearParquet::PrimitiveNode::Make("Timestamp", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));
    columnNames.push_back(clearParquet::PrimitiveNode::Make("SaveStateReason", clearParquet::Repetition::REQUIRED, clearParquet::Type::BYTE_ARRAY,
                                                            clearParquet::ConvertedType::UTF8));
    columnNames.push_back(clearParquet::PrimitiveNode::Make("AnotherTimestamp", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64,
                                                            clearParquet::ConvertedType::UINT_64));
    columnNames.push_back(clearParquet::PrimitiveNode::Make("SaveStateReasonabcd", clearParquet::Repetition::REQUIRED, clearParquet::Type::BYTE_ARRAY,
                                                            clearParquet::ConvertedType::UTF8));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));

    clearParquet::WriterProperties::Builder builder;
    builder.compression(clearParquet::Compression::SNAPPY);

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());

    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};

    writer.SetMaxRowGroupSize(1ll * 1024ll * 1024ll);

    std::string saveStateReason = "SaveStateReasonData";
    // uint64_t currentTimeNs = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    uint64_t currentTimeNs = 0xDEADBEEF;
    uint64_t b = 0xaaaaaaaa;
    std::cout << currentTimeNs << std::endl;
    writer << currentTimeNs << saveStateReason.c_str() << b << saveStateReason.c_str() << clearParquet::EndRow;
    // writer << currentTimeNs << clearParquet::EndRow;
    // writer << currentTimeNs << clearParquet::EndRow;
    currentTimeNs = 0xffffffff;
    // writer << currentTimeNs << clearParquet::EndRow;
    saveStateReason = "SOMENEWDATA";
    b = 0xbbbbbbbb;
    for (int i = 0; i < 100000000; ++i) {
        // writer << currentTimeNs << clearParquet::EndRow;
        writer << currentTimeNs << saveStateReason.c_str() << b << saveStateReason.c_str() << clearParquet::EndRow;
    }

    return 0;
}
