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

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build(), 2048);

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
        writer << INT64_MAX-(row++) << clearParquet::EndRow;
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
            REQUIRE(std::any_cast<uint64_t>(batch->Column(i)->Value(batch->Column(i)->Size()-1)) == INT64_MAX-99);
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

TEST_CASE("Vector Read") {
    WriteSimpleRows(100);
    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        for (uint64_t i = 0; i < batch->NumColumns(); ++i) {
            std::shared_ptr<std::vector<uint64_t>> data;
            REQUIRE(batch->Column(i)->ToVector(data) == true);
            REQUIRE(data->size() == 100);
        }
    }
}

TEST_CASE("ReadWrite Int32") {    
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("UnitTest_Int32", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT32, clearParquet::ConvertedType::INT_32));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    int32_t rows = 100;
    for (int32_t i = 0; i < rows; ++i) {
        writer << static_cast<int32_t>(i) << clearParquet::EndRow;
    }
    writer.Close();

    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        size_t num_cols = batch->NumColumns();
        REQUIRE(num_cols == 1);
        auto column_name = batch->Column(0)->ToString();
        REQUIRE(column_name == "UnitTest_Int32");
        REQUIRE(batch->Column(0)->Size() == static_cast<uint64_t>(rows));
        REQUIRE(std::any_cast<int32_t>(batch->Column(0)->Value(rows - 1)) == (rows - 1));
    }
}

TEST_CASE("ReadWrite Int64") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("UnitTest_Int64", clearParquet::Repetition::REQUIRED, clearParquet::Type::INT64, clearParquet::ConvertedType::INT_64));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    int64_t rows = 100;
    for (int64_t i = 0; i < rows; ++i) {
        writer << static_cast<int64_t>(i) << clearParquet::EndRow;
    }
    writer.Close();

    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        size_t num_cols = batch->NumColumns();
        REQUIRE(num_cols == 1);
        auto column_name = batch->Column(0)->ToString();
        REQUIRE(column_name == "UnitTest_Int64");
        REQUIRE(batch->Column(0)->Size() == static_cast<uint64_t>(rows));
        REQUIRE(static_cast<int64_t>(std::any_cast<uint64_t>(batch->Column(0)->Value(rows - 1))) == (rows - 1));
    }
}

TEST_CASE("ReadWrite Double") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("UnitTest_Double", clearParquet::Repetition::REQUIRED, clearParquet::Type::DOUBLE, clearParquet::ConvertedType::NONE));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    int64_t rows = 100;
    for (int64_t i = 0; i < rows; ++i) {
        writer << static_cast<double>(i) << clearParquet::EndRow;
    }
    writer.Close();

    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        size_t num_cols = batch->NumColumns();
        REQUIRE(num_cols == 1);
        auto column_name = batch->Column(0)->ToString();
        REQUIRE(column_name == "UnitTest_Double");
        REQUIRE(batch->Column(0)->Size() == static_cast<uint64_t>(rows));
        REQUIRE(std::any_cast<double>(batch->Column(0)->Value(rows - 1)) == static_cast<double>(rows - 1));
    }
}

TEST_CASE("ReadWrite Bool") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("UnitTest_Bool", clearParquet::Repetition::REQUIRED, clearParquet::Type::BOOLEAN, clearParquet::ConvertedType::NONE));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    int64_t rows = 100;
    for (int64_t i = 0; i < rows; ++i) {
        bool value = static_cast<bool>(i % 2);
        writer << value << clearParquet::EndRow;
    }
    writer.Close();

    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        size_t num_cols = batch->NumColumns();
        REQUIRE(num_cols == 1);
        auto column_name = batch->Column(0)->ToString();
        REQUIRE(column_name == "UnitTest_Bool");
        REQUIRE(batch->Column(0)->Size() == static_cast<uint64_t>(rows));
        REQUIRE(std::any_cast<bool>(batch->Column(0)->Value(rows - 1)) == static_cast<bool>((rows - 1) % 2));
    }
}

TEST_CASE("ReadWrite ByteArray") {
    std::shared_ptr<clearParquet::FileOutputStream> outfile = clearParquet::FileOutputStream::Open("test_output.parquet");
    clearParquet::NodeVector columnNames{};
    columnNames.push_back(clearParquet::PrimitiveNode::Make("UnitTest_String", clearParquet::Repetition::REQUIRED, clearParquet::Type::BYTE_ARRAY, clearParquet::ConvertedType::UTF8));

    auto schema = std::static_pointer_cast<clearParquet::GroupNode>(clearParquet::GroupNode::Make("schema", clearParquet::Repetition::REQUIRED, columnNames));
    clearParquet::WriterProperties::Builder builder;

    std::unique_ptr<clearParquet::ParquetFileWriter> fwriter = clearParquet::ParquetFileWriter::Open(outfile, schema, builder.build());
    clearParquet::StreamWriter writer = clearParquet::StreamWriter{std::move(fwriter)};
    int64_t rows = 100;    
    char value[64];
    for (int64_t i = 0; i < rows; ++i) {
        memset(value, 0, sizeof(value));
        snprintf(value, sizeof(value), "value %ld", i);
        writer << value << clearParquet::EndRow;
    }
    writer.Close();

    std::shared_ptr<clearParquet::FileInputStream> infile = clearParquet::FileInputStream::Open("test_output.parquet");
    auto reader = clearParquet::ParquetFileReader::Open(infile);
    for (const auto& batch : *reader) {
        size_t num_cols = batch->NumColumns();
        REQUIRE(num_cols == 1);
        auto column_name = batch->Column(0)->ToString();
        REQUIRE(column_name == "UnitTest_String");
        REQUIRE(batch->Column(0)->Size() == static_cast<uint64_t>(rows));
        // TODO: Use strcmp() to test value.
    }
}
