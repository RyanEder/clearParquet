#include <iostream>

#include "arrow/io/file.h"
#include "parquet/stream_writer.h"
#include "parquet/arrow/writer.h"
#include "arrow/util/type_fwd.h"


int main(int argc, char **argv) {
    uint32_t numRows = 1000;
    if (argc >= 2) {
        numRows = atoi(argv[1]);
    }

    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    
    PARQUET_ASSIGN_OR_THROW(
      outfile,
      arrow::io::FileOutputStream::Open("test_output.parquet"));

    parquet::schema::NodeVector columnNames{};

    columnNames.push_back(parquet::schema::PrimitiveNode::Make("Timestamp", parquet::Repetition::REQUIRED, parquet::Type::INT64, parquet::ConvertedType::UINT_64));
    columnNames.push_back(parquet::schema::PrimitiveNode::Make("SaveStateReason", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));
    columnNames.push_back(parquet::schema::PrimitiveNode::Make("AnotherTimestamp", parquet::Repetition::REQUIRED, parquet::Type::INT64, parquet::ConvertedType::UINT_64));
    columnNames.push_back(parquet::schema::PrimitiveNode::Make("SaveStateReasonabcd", parquet::Repetition::REQUIRED, parquet::Type::BYTE_ARRAY, parquet::ConvertedType::UTF8));


    auto schema = std::static_pointer_cast<parquet::schema::GroupNode>(parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, columnNames));

    parquet::WriterProperties::Builder builder;
    if (argc <= 2) {
        builder.compression(parquet::Compression::UNCOMPRESSED);
    } else if (atoi(argv[2]) == 1) {
        builder.compression(parquet::Compression::SNAPPY);
    } else if (atoi(argv[2]) == 2) {
        builder.compression(parquet::Compression::ZSTD);
    } else {
        builder.compression(parquet::Compression::UNCOMPRESSED);
    }

    builder.disable_dictionary();
    builder.disable_statistics();
    builder.disable_page_checksum();

    std::unique_ptr<parquet::ParquetFileWriter> fwriter = parquet::ParquetFileWriter::Open(outfile, schema, builder.build());

    parquet::StreamWriter writer = parquet::StreamWriter { std::move(fwriter) };

    writer.SetMaxRowGroupSize(10ll*1024ll*1024ll);

    std::string saveStateReason = "SaveStateReasonData";
    uint64_t a = 0xffffffff;
    uint64_t b = 0xaaaaaaaa;
    for (uint32_t i = 0; i < numRows; ++i) {
        writer << a-- << saveStateReason.c_str() << b++ << saveStateReason.c_str() << parquet::EndRow;
    }

    return 0;
}
