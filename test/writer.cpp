#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "ParquetColumnChunk.hpp"
#include "ParquetPageHeader.hpp"
#include "ParquetStreamWriter.hpp"

int main(int argc, char **argv) {
    uint32_t numRows = 1000;
    if (argc >= 2) {
        numRows = atoi(argv[1]);
    }

    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");

    clearParquet::NodeVector columnNames{};

    columnNames.push_back(clearParquet::PrimitiveNode::Make("Timestamp", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));
    columnNames.push_back(clearParquet::PrimitiveNode::Make("SaveStateReason", clearParquet::Repetition::REQUIRED, clearParquet::Type::BYTE_ARRAY,
                                                            clearParquet::ConvertedType::UTF8));
    columnNames.push_back(clearParquet::PrimitiveNode::Make("AnotherTimestamp", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64,
                                                            clearParquet::ConvertedType::UINT_64));
    columnNames.push_back(clearParquet::PrimitiveNode::Make("SaveStateReasonabcd", clearParquet::Repetition::REQUIRED, clearParquet::Type::BYTE_ARRAY,
                                                            clearParquet::ConvertedType::UTF8));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));

    clearParquet::WriterProperties::Builder builder;
    if (argc <= 2) {
        builder.compression(clearParquet::Compression::UNCOMPRESSED);
    } else if (atoi(argv[2]) == 1) {
        builder.compression(clearParquet::Compression::SNAPPY);
    } else if (atoi(argv[2]) == 2) {
        builder.compression(clearParquet::Compression::ZSTD);
    } else {
        builder.compression(clearParquet::Compression::UNCOMPRESSED);
    }

    builder.disable_dictionary();
    builder.disable_statistics();
    builder.disable_page_checksum();

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());

    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};

    writer.SetMaxRowGroupSize(1ll * 1024ll * 1024ll); // 1MB strides

    std::string saveStateReason = "SaveStateReasonData";
    uint64_t a = 0xffffffff;
    uint64_t b = 0xaaaaaaaa;
    for (uint32_t i = 0; i < numRows; ++i) {
        writer << a-- << saveStateReason.c_str() << b++ << saveStateReason.c_str() << clearParquet::EndRow;
    }

    return 0;
}
