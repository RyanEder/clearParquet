#include <vector>
#include <memory>
#include <iostream>

#include <catch2/catch_test_macros.hpp>

#include "ParquetColumnChunk.hpp" 
#include "ParquetPageHeader.hpp"  
#include "ParquetStreamWriter.hpp"

#include "ParquetFileReader.hpp"

TEST_CASE("FileOutputStream") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");

    REQUIRE(outfile != nullptr);
}

TEST_CASE("Schema") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));

    REQUIRE(schema != nullptr);
}

TEST_CASE("FWriter") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());

    REQUIRE(fwriter != nullptr);
}

TEST_CASE("Reader") {
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");

    REQUIRE(infile != nullptr);
}

TEST_CASE("Valid Schema") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    writer << (uint64_t)0x01 << clearParquet::EndRow;
    writer.Close();

    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    auto readSchema = reader->schema();

    REQUIRE(readSchema.size() == 2);
}

TEST_CASE("ZSTD") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;
    builder.compression(clearParquet::Compression::ZSTD);
    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    writer << (uint64_t)0x01 << clearParquet::EndRow;
    writer.Close();
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
}

TEST_CASE("SNAPPY") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;
    builder.compression(clearParquet::Compression::SNAPPY);
    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    writer << (uint64_t)0x01 << clearParquet::EndRow;
    writer.Close();
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
}

void WriteSimpleRows(uint64_t rows) {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    uint64_t row = 0;
    for (uint64_t i = 0; i < rows; ++i) {
        writer << row++ << clearParquet::EndRow;
    }
    writer.Close();
}

TEST_CASE("1 Row") {
    WriteSimpleRows(1);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);

    REQUIRE(reader->NumRows() == 1);
}

TEST_CASE("1000 Rows") {
    WriteSimpleRows(1000);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);

    REQUIRE(reader->NumRows() == 1000);
}

TEST_CASE("10,000 Rows") {
    WriteSimpleRows(10000);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);

    REQUIRE(reader->NumRows() == 10000);
}

TEST_CASE("100,000 Rows") {
    WriteSimpleRows(100000);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);

    REQUIRE(reader->NumRows() == 100000);
}

TEST_CASE("1,000,000 Rows") {
    WriteSimpleRows(1000000);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);

    REQUIRE(reader->NumRows() == 1000000);
}

TEST_CASE("Read Values") {
    WriteSimpleRows(100);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        for (uint64_t i = 0; i < batch->NumColumns(); ++i) {
            REQUIRE(batch->Column(i)->ToString() == "Unittest");
            REQUIRE(std::any_cast<uint64_t>(batch->Column(i)->Value(batch->Column(i)->Size()-1)) == 99);
        }
    }
}

TEST_CASE("ZSTD Read") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;
    builder.compression(clearParquet::Compression::ZSTD);
    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    writer << (uint64_t)0x01 << clearParquet::EndRow;
    writer.Close();
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        for (uint64_t i = 0; i < batch->NumColumns(); ++i) {
            REQUIRE(batch->Column(i)->ToString() == "Unittest");
            REQUIRE(std::any_cast<uint64_t>(batch->Column(i)->Value(batch->Column(i)->Size()-1)) == 1);
        }
    }
}

TEST_CASE("Snappy Read") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("Unittest", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::UINT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;
    builder.compression(clearParquet::Compression::SNAPPY);
    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    writer << (uint64_t)0x01 << clearParquet::EndRow;
    writer.Close();
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        for (uint64_t i = 0; i < batch->NumColumns(); ++i) {
            REQUIRE(batch->Column(i)->ToString() == "Unittest");
            REQUIRE(std::any_cast<uint64_t>(batch->Column(i)->Value(batch->Column(i)->Size()-1)) == 1);
        }
    }
}
