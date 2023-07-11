#pragma once
#include <cstdint>
#include <fstream>
#include <iostream>

#include "ParquetDataStore.hpp"
#include "ParquetFileOutputStream.hpp"
#include "ParquetSchema.hpp"
#include "ParquetThriftSerializer.hpp"
#include "ParquetWriterProperties.hpp"

namespace clearParquet {

class ParquetFileWriter {
   public:
    ParquetFileWriter(std::shared_ptr<FileOutputStream> filename, ParquetSchema& schema, std::shared_ptr<WriterProperties> properties)
        : _filename(filename), _schema(schema) {
        try {
            _file.open(filename->filename(), std::ios::out | std::ios::binary);
        } catch (...) {
            std::cout << "Cannot open file." << filename->filename() << std::endl;
            _opened = false;
        }
        SchemaBuilder builder;
        builder.Visit(_schema.get());
        _schemas = builder.GetSchemas();
        uint32_t counts[(uint8_t)Type::NONE] = {0};
        for (uint32_t i = 0; i < _schemas.size() - 1; ++i) {
            const auto& element = _schemas[i + 1];
            counts[(uint8_t)element._type]++;
        }
        for (uint8_t utype = 0; utype < (uint8_t)Type::NONE; ++utype) {
            if (counts[utype] > 0) {
                Type::type ctype = (Type::type)utype;
                switch (ctype) {
                    case Type::BOOLEAN:
                        _boolCols = std::make_shared<DataStore<bool>>(1024 * 1024, counts[utype]);
                        break;
                    case Type::INT32:
                        _int32Cols = std::make_shared<DataStore<int32_t>>(1024 * 1024, counts[utype]);
                        break;
                    case Type::INT64:
                        _int64Cols = std::make_shared<DataStore<int64_t>>(1024 * 1024, counts[utype]);
                        break;
                    case Type::INT96:
                        // Unsupported
                        break;
                    case Type::FLOAT:
                        _floatCols = std::make_shared<DataStore<float>>(1024 * 1024, counts[utype]);
                        break;
                    case Type::DOUBLE:
                        _doubleCols = std::make_shared<DataStore<double>>(1024 * 1024, counts[utype]);
                        break;
                    case Type::BYTE_ARRAY:
                        _strCols = std::make_shared<DataStore<std::string>>(1024 * 1024, counts[utype]);
                        break;
                    case Type::FIXED_LEN_BYTE_ARRAY:
                        // Unsupported
                        break;
                    default:
                        break;
                }
            }
        }
        StartFile();
    }
    ~ParquetFileWriter() {
        EndFile();
        try {
            if (_opened) {
                _file.close();
            }
        } catch (...) {}
    }

    void StartFile() {
        if (_opened) {
            _file.clear();
            _file.seekg(0, std::ios::beg);
            _file.write(PARQUET_MAGIC, 4);
        }

        _encodings.push_back(clearParquet::Encoding::RLE);
        _encodings.push_back(clearParquet::Encoding::PLAIN);

        ColumnOrder order;
        TypeDefinedOrder type_defined_order;
        order.__set_TYPE_ORDER(type_defined_order);

        // For the metadata later
        for (uint32_t i = 0; i < _schemas.size() - 1; ++i) {
            _columnOrders.push_back(order);
        }
    }

    template <class T>
    int64_t SerializeAndWrite(const T* obj) {
        uint8_t* out_buffer;
        uint32_t out_length;

        _serializer.SerializeToBuffer(obj, &out_length, &out_buffer);
        WriteBuffer(out_length, out_buffer, false);
        return static_cast<uint64_t>(out_length);
    }

    void WriteBuffer(uint32_t len, const uint8_t* buffer, bool shouldBuffer = true) {
        if (shouldBuffer) {
            if (cache + len >= max) {
                _file.write((const char*)buf, cache);
                cache = 0;
            }
            memcpy(buf + cache, buffer, len);
            cache += len;
        } else {
            if (cache > 0) {
                _file.write((const char*)buf, cache);
                cache = 0;
            }
            _file.write((const char*)buffer, len);
        }
    }

    void WriteFileMetaData() {
        uint32_t metadata_len = static_cast<uint32_t>(_file.tellg());
        SerializeAndWrite(&_fileMetaData);
        metadata_len = static_cast<uint32_t>(_file.tellg()) - metadata_len;
        _file.write(reinterpret_cast<const char*>(&metadata_len), 4);
        _file.write(PARQUET_MAGIC, 4);
    }

    uint64_t GetSize() {
        uint64_t dataLen = 0;
        for (uint32_t i = 0; i < _schemas.size() - 1; ++i) {
            const auto& element = _schemas[i + 1];
            switch (element._type) {
                case Type::BOOLEAN:
                    dataLen += _boolCols->GetSize();
                    break;
                case Type::INT32:
                    dataLen += _int32Cols->GetSize();
                    break;
                case Type::INT64:
                    dataLen += _int64Cols->GetSize();
                    break;
                case Type::INT96:
                    // Unsupported
                    break;
                case Type::FLOAT:
                    dataLen += _floatCols->GetSize();
                    break;
                case Type::DOUBLE:
                    dataLen += _doubleCols->GetSize();
                    break;
                case Type::BYTE_ARRAY:
                    dataLen += _strCols->GetSize();
                    break;
                case Type::FIXED_LEN_BYTE_ARRAY:
                    // Unsupported
                    break;
                default:
                    break;
            }
        }
        return dataLen;
    }

    void ClearStorage() {
        _numRows = 0;
        for (uint32_t i = 0; i < _schemas.size() - 1; ++i) {
            const auto& element = _schemas[i + 1];
            switch (element._type) {
                case Type::BOOLEAN:
                    _boolCols->Clear();
                    break;
                case Type::INT32:
                    _int32Cols->Clear();
                    break;
                case Type::INT64:
                    _int64Cols->Clear();
                    break;
                case Type::INT96:
                    // Unsupported
                    break;
                case Type::FLOAT:
                    _floatCols->Clear();
                    break;
                case Type::DOUBLE:
                    _doubleCols->Clear();
                    break;
                case Type::BYTE_ARRAY:
                    _strCols->Clear();
                    break;
                case Type::FIXED_LEN_BYTE_ARRAY:
                    // Unsupported
                    break;
                default:
                    break;
            }
        }
    }

    void EndRowGroup() {
        uint32_t initialOffset = static_cast<uint32_t>(_file.tellg());
        uint64_t rowBytes = 0;

        _dataPageHeader.__set_num_values(_numRows);
        _dataPageHeader.__set_definition_level_encoding(clearParquet::Encoding::RLE);
        _dataPageHeader.__set_repetition_level_encoding(clearParquet::Encoding::RLE);
        _dataPageHeader.__set_statistics(_stats);
        for (uint32_t i = 0; i < _schemas.size() - 1; ++i) {
            const auto& element = _schemas[i + 1];
            PageHeader header;

            uint32_t dataLen = 0;
            switch (element._type) {
                case Type::BOOLEAN:
                    dataLen = _boolCols->GetSize();
                    break;
                case Type::INT32:
                    dataLen = _int32Cols->GetSize();
                    break;
                case Type::INT64:
                    dataLen = _int64Cols->GetSize();
                    break;
                case Type::INT96:
                    // Unsupported
                    break;
                case Type::FLOAT:
                    dataLen = _floatCols->GetSize();
                    break;
                case Type::DOUBLE:
                    dataLen = _doubleCols->GetSize();
                    break;
                case Type::BYTE_ARRAY:
                    dataLen = _strCols->GetSize();
                    break;
                case Type::FIXED_LEN_BYTE_ARRAY:
                    // Unsupported
                    break;
                default:
                    break;
            }
            header.__set_compressed_page_size(dataLen);
            header.__set_uncompressed_page_size(dataLen);
            header.__set_data_page_header(_dataPageHeader);
            // Write Header
            uint32_t dataPageOffset = static_cast<uint32_t>(_file.tellg());
            SerializeAndWrite(&header);
            uint32_t pageHeaderLen = static_cast<uint32_t>(_file.tellg()) - dataPageOffset;

            ColumnChunk chunk;
            ColumnMetaData cmd;
            std::vector<std::string> path_in_schema;
            std::vector<PageEncodingStats> page_encodings;
            path_in_schema.push_back(element._name);
            PageEncodingStats page_encoding_stats;
            page_encoding_stats.__set_page_type(PageType::DATA_PAGE);
            page_encoding_stats.__set_encoding(Encoding::PLAIN);
            page_encoding_stats.__set_count(1);  // EDER INVESTIGATE

            page_encodings.push_back(page_encoding_stats);
            chunk.__set_file_offset(dataLen + pageHeaderLen + dataPageOffset);
            cmd.__set_type(element._type);
            cmd.__set_encodings(_encodings);
            cmd.__set_path_in_schema(path_in_schema);
            cmd.__set_codec(CompressionCodec::UNCOMPRESSED);
            cmd.__set_num_values(_numRows);
            cmd.__set_total_uncompressed_size(dataLen + pageHeaderLen);
            cmd.__set_total_compressed_size(dataLen + pageHeaderLen);
            cmd.__set_data_page_offset(dataPageOffset);
            cmd.__set_encoding_stats(page_encodings);
            chunk.__set_meta_data(cmd);
            _columnChunks.push_back(chunk);

            rowBytes += dataLen + pageHeaderLen;

            switch (element._type) {
                case Type::BOOLEAN:
                    for (const auto& val : _boolCols->Get()) {
                        WriteBuffer(1, (const uint8_t*)&val);
                    }
                    break;
                case Type::INT32:
                    for (const auto& val : _int32Cols->Get()) {
                        WriteBuffer(4, (const uint8_t*)&val);
                    }
                    break;
                case Type::INT64:
                    for (const auto& val : _int64Cols->Get()) {
                        WriteBuffer(8, (const uint8_t*)&val);
                    }
                    break;
                case Type::INT96:
                    // Unsupported
                    break;
                case Type::FLOAT:
                    for (const auto& val : _floatCols->Get()) {
                        WriteBuffer(4, (const uint8_t*)&val);
                    }
                    break;
                case Type::DOUBLE:
                    for (const auto& val : _doubleCols->Get()) {
                        WriteBuffer(8, (const uint8_t*)&val);
                    }
                    break;
                case Type::BYTE_ARRAY:
                    for (const auto& str : _strCols->Get()) {
                        uint32_t len = str.length();
                        WriteBuffer(4, (const uint8_t*)&len);
                        WriteBuffer(len, (const uint8_t*)str.c_str());
                    }
                    break;
                case Type::FIXED_LEN_BYTE_ARRAY:
                    // Unsupported
                    break;
                default:
                    break;
            }
            SerializeAndWrite(&chunk);
        }
        RowGroup row_group;
        row_group.__set_columns(_columnChunks);
        row_group.__set_total_byte_size(rowBytes);
        row_group.__set_num_rows(_numRows);
        row_group.__set_file_offset(initialOffset);
        row_group.__set_total_compressed_size(rowBytes);
        row_group.__set_ordinal(_rowGroups.size());
        _rowGroups.push_back(row_group);

        ClearStorage();
        _columnChunks.clear();
    }

    void FinishFileMetaData() {
        _fileMetaData.__set_version(2);
        _fileMetaData.__set_schema(_schemas);
        _fileMetaData.__set_num_rows(_totalRows);
        _fileMetaData.__set_created_by(_createdBy);
        _fileMetaData.__set_row_groups(_rowGroups);
        _fileMetaData.__set_column_orders(_columnOrders);
    }

    void EndFile() {
        if (_opened) {
            EndRowGroup();
            FinishFileMetaData();
            WriteFileMetaData();
        }
    }

    void EndRow() {
        ++_numRows;
        ++_totalRows;
    }

    static std::unique_ptr<ParquetFileWriter> Open(std::shared_ptr<FileOutputStream> filename, ParquetSchema& schema,
                                                   std::shared_ptr<WriterProperties> properties) {
        std::unique_ptr<ParquetFileWriter> result(new ParquetFileWriter(filename, schema, properties));
        return result;
    }

   private:
    std::shared_ptr<FileOutputStream> _filename;
    ParquetSchema& _schema;
    std::fstream _file;
    bool _opened = true;
    ThriftSerializer _serializer;
    uint64_t _numRows = 0;
    uint64_t _totalRows = 0;
    FileMetaData _fileMetaData;
    std::vector<SchemaElement> _schemas;
    std::vector<ColumnChunk> _columnChunks;
    std::vector<ColumnOrder> _columnOrders;
    std::vector<RowGroup> _rowGroups;

    std::vector<Encoding::type> _encodings;

    uint64_t max = 10 * 1024 * 1024;
    char buf[10 * 1024 * 1024];
    uint64_t cache = 0;
    Statistics _stats;
    DataPageHeader _dataPageHeader;
    std::string _createdBy = "clearParquet version 1.0.0";
    // std::string _createdBy = "parquet-cpp-arrow version 13.0.0-SNAPSHOT";

   public:
    std::shared_ptr<DataStore<bool>> _boolCols;
    std::shared_ptr<DataStore<float>> _floatCols;
    std::shared_ptr<DataStore<double>> _doubleCols;
    std::shared_ptr<DataStore<int32_t>> _int32Cols;
    std::shared_ptr<DataStore<int64_t>> _int64Cols;
    std::shared_ptr<DataStore<std::string>> _strCols;
};

}  // end namespace clearParquet
