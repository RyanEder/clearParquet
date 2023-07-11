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
constexpr size_t INTERNAL_BUFFER_SIZE = 10LL * 1024LL * 1024LL;
constexpr size_t INITIAL_RESERVE_SIZE = 1024LL * 1024LL;
constexpr std::string_view CREATED_BY = "clearParquet version 1.0.0";

class ParquetFileWriter {
    // File Format:
    // PAR1 -- Magic bytes indicating parquet file.
    // ForEach RowGroup:
    //   ForEach Column in a RowGroup:
    //     Page Header metadata
    //     Data (if byte array, each element has a length field before)
    //     Column Chunk metadata
    // FileMetaData
    // File Length
    // PAR1 -- Magic bytes again indicating parquet file end.

public:
    ParquetFileWriter(const std::shared_ptr<FileOutputStream>& filename, ParquetSchema& schema, const std::shared_ptr<WriterProperties>& properties)
        : _filename(filename), _schema(schema), _opened(true), _numRows(0), _totalRows(0), _cacheIndex(0), _createdBy(CREATED_BY) {
        // Open the file
        try {
            _file.open(filename->filename(), std::ios::out | std::ios::binary);
        } catch (...) {
            std::cout << "Cannot open file." << filename->filename() << std::endl;
            _opened = false;
        }
        // Collect the schema for storage requirements
        SchemaBuilder builder;
        builder.Visit(_schema.get());
        _schemas = builder.GetSchemas();
        uint32_t counts[(uint8_t)Type::NONE] = {0};
        for (uint32_t i = 0; i < _schemas.size() - 1; ++i) {
            const auto& element = _schemas[i + 1];
            counts[(uint8_t)element._type]++;
        }
        // Build out initial storage.
        for (uint8_t utype = 0; utype < (uint8_t)Type::NONE; ++utype) {
            if (counts[utype] > 0) {
                Type::type ctype = (Type::type)utype;
                switch (ctype) {
                    case Type::BOOLEAN:
                        _boolCols = std::make_shared<DataStore<bool>>(INITIAL_RESERVE_SIZE, counts[utype]);
                        break;
                    case Type::INT32:
                        _int32Cols = std::make_shared<DataStore<int32_t>>(INITIAL_RESERVE_SIZE, counts[utype]);
                        break;
                    case Type::INT64:
                        _int64Cols = std::make_shared<DataStore<int64_t>>(INITIAL_RESERVE_SIZE, counts[utype]);
                        break;
                    case Type::INT96:
                        // Unsupported
                        break;
                    case Type::FLOAT:
                        _floatCols = std::make_shared<DataStore<float>>(INITIAL_RESERVE_SIZE, counts[utype]);
                        break;
                    case Type::DOUBLE:
                        _doubleCols = std::make_shared<DataStore<double>>(INITIAL_RESERVE_SIZE, counts[utype]);
                        break;
                    case Type::BYTE_ARRAY:
                        _strCols = std::make_shared<DataStore<std::string>>(INITIAL_RESERVE_SIZE, counts[utype]);
                        break;
                    case Type::FIXED_LEN_BYTE_ARRAY:
                        // Unsupported
                        break;
                    default:
                        break;
                }
            }
        }
        // Write the beginning of the file
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

        // Set up elements for later
        _encodings.push_back(clearParquet::Encoding::RLE);
        _encodings.push_back(clearParquet::Encoding::PLAIN);

        PageEncodingStats pageEncodingStats;
        pageEncodingStats.__set_page_type(PageType::DATA_PAGE);
        pageEncodingStats.__set_encoding(Encoding::PLAIN);
        pageEncodingStats.__set_count(1);
        _pageEncodings.push_back(pageEncodingStats);
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
        uint8_t* outBuffer;
        uint32_t outLength;

        _serializer.SerializeToBuffer(obj, &outLength, &outBuffer);
        WriteBuffer(outLength, outBuffer, false);
        return static_cast<uint64_t>(outLength);
    }

    void WriteBuffer(uint32_t len, const uint8_t* buffer, bool shouldBuffer = true) {
        if (shouldBuffer) {
            if (_cacheIndex + len >= INTERNAL_BUFFER_SIZE) {
                _file.write((const char*)_cacheBuffer, _cacheIndex);
                _cacheIndex = 0;
            }
            memcpy(_cacheBuffer + _cacheIndex, buffer, len);
            _cacheIndex += len;
        } else {
            if (_cacheIndex > 0) {
                _file.write((const char*)_cacheBuffer, _cacheIndex);
                _cacheIndex = 0;
            }
            _file.write((const char*)buffer, len);
        }
    }

    void WriteFileMetaData() {
        // Calculate and finish the file.
        uint32_t metadataLen = static_cast<uint32_t>(_file.tellg());
        SerializeAndWrite(&_fileMetaData);
        metadataLen = static_cast<uint32_t>(_file.tellg()) - metadataLen;
        _file.write(reinterpret_cast<const char*>(&metadataLen), 4);
        _file.write(PARQUET_MAGIC, 4);
    }

    uint64_t GetSize() {
        // Figure out data sizes
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
        // Zero structures, as this data has already been stored to disk.
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
        // Figure and write a row group of column chunks
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
            std::vector<std::string> pathInSchema;
            pathInSchema.push_back(element._name);

            chunk.__set_file_offset(dataLen + pageHeaderLen + dataPageOffset);
            cmd.__set_type(element._type);
            cmd.__set_encodings(_encodings);
            cmd.__set_path_in_schema(pathInSchema);
            cmd.__set_codec(CompressionCodec::UNCOMPRESSED);
            cmd.__set_num_values(_numRows);
            cmd.__set_total_uncompressed_size(dataLen + pageHeaderLen);
            cmd.__set_total_compressed_size(dataLen + pageHeaderLen);
            cmd.__set_data_page_offset(dataPageOffset);
            cmd.__set_encoding_stats(_pageEncodings);
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

    static std::unique_ptr<ParquetFileWriter> Open(const std::shared_ptr<FileOutputStream>& filename, ParquetSchema& schema,
                                                   const std::shared_ptr<WriterProperties>& properties) {
        std::unique_ptr<ParquetFileWriter> result(new ParquetFileWriter(filename, schema, properties));
        return result;
    }

private:
    std::shared_ptr<FileOutputStream> _filename;
    ParquetSchema& _schema;
    std::fstream _file;
    bool _opened;
    ThriftSerializer _serializer;
    uint64_t _numRows;
    uint64_t _totalRows;
    FileMetaData _fileMetaData;
    std::vector<SchemaElement> _schemas;
    std::vector<ColumnChunk> _columnChunks;
    std::vector<ColumnOrder> _columnOrders;
    std::vector<RowGroup> _rowGroups;
    std::vector<PageEncodingStats> _pageEncodings;

    std::vector<Encoding::type> _encodings;

    char _cacheBuffer[INTERNAL_BUFFER_SIZE];
    uint64_t _cacheIndex;
    Statistics _stats;
    DataPageHeader _dataPageHeader;
    std::string _createdBy;

public:
    std::shared_ptr<DataStore<bool>> _boolCols;
    std::shared_ptr<DataStore<float>> _floatCols;
    std::shared_ptr<DataStore<double>> _doubleCols;
    std::shared_ptr<DataStore<int32_t>> _int32Cols;
    std::shared_ptr<DataStore<int64_t>> _int64Cols;
    std::shared_ptr<DataStore<std::string>> _strCols;
};

}  // end namespace clearParquet
