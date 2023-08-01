#pragma once
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

#include "ParquetFileInputStream.hpp"
#include "ParquetRecordBatch.hpp"
#include "ParquetSchema.hpp"
#include "ParquetThriftDecoder.hpp"

namespace clearParquet {

class ParquetFileReader {
public:
    using Iterator = std::vector<std::shared_ptr<RecordBatch>>::iterator;
    Iterator begin() {
        return _records.begin();
    }
    Iterator end() {
        return _records.end();
    }

    ParquetFileReader(const std::shared_ptr<FileInputStream>& filename)
        : _filename(filename), _opened(true), _numRows(0), _buffer(nullptr), _currentBufferLen(0) {
        try {
            _file.open(filename->filename(), std::ios::in | std::ios::binary);
        } catch (...) {
            _opened = false;
            throw std::invalid_argument(std::string("Cannot open file: ") + filename->filename());
        }
    }

    static std::unique_ptr<ParquetFileReader> Open(const std::shared_ptr<FileInputStream>& filename) {
        std::unique_ptr<ParquetFileReader> result(new ParquetFileReader(filename));
        result->ReadFileMetadata();
        return result;
    }

    ~ParquetFileReader() {
        try {
            if (_opened) {
                _file.close();
            }
        } catch (...) {}
    }

    ParquetSchema& schema() {
        return _schema;
    }

    void ReadFileMetadata() {
        if (!_opened) {
            throw std::runtime_error("Cannot read the schema on a closed file.");
        }
        char magicBytes[4] = {'\0', '\0', '\0', '\0'};
        _file.seekg(0, std::ios::beg);
        _file.read(reinterpret_cast<char*>(&magicBytes), 4);  // First 4 are PAR1, check it.
        MagicCheck(magicBytes);
        _file.seekg(0, std::ios::end);
        if (_file.tellg() >= 12) {           // Magic bytes for PAR1, 2X plus the file size.
            _file.seekg(-4, std::ios::end);  // Last 4 are PAR1, check it.
            _file.read(reinterpret_cast<char*>(&magicBytes), 4);
            MagicCheck(magicBytes);

            int32_t metadataLen = 0;
            _file.seekg(-8, std::ios::end);  // Behind the PAR1 is the metadata length
            _file.read(reinterpret_cast<char*>(&metadataLen), sizeof(metadataLen));

            // From the end, the start of the metadata is 8 bytes plus the size
            _file.seekg(-metadataLen - 8, std::ios::end);
            _buffer = new char[metadataLen];
            _currentBufferLen = metadataLen;
            _file.read(reinterpret_cast<char*>(_buffer), metadataLen);
            int32_t count = 0;
            _decoder.setBuffer(_buffer);
            _decoder.decodeFieldBegin();  // version i32
            _fileMetaData.__set_version(_decoder.decodeI32());
            _decoder.decodeFieldBegin();      // schema struct
            _decoder.decodeListBegin(count);  // schema list count

            // Schema section
            SchemaElement elem;
            _decoder.decodeFieldBegin();  // rep type i32
            elem.__set_repetition_type((FieldRepetitionType::type)_decoder.decodeI32());
            _decoder.decodeFieldBegin();  // name str
            elem.__set_name(_decoder.decodeString());
            _decoder.decodeFieldBegin();  // num_children i32
            elem.__set_num_children(_decoder.decodeI32());
            _decoder.decodeFieldStop();
            _schemas.push_back(elem);

            // 0th schema special, these are all the same now.
            for (int i = 1; i < count; ++i) {
                SchemaElement element;
                _decoder.decodeFieldBegin();  // type i32
                element.__set_type((Type::type)_decoder.decodeI32());
                _decoder.decodeFieldBegin();  // rep type i32
                element.__set_repetition_type((FieldRepetitionType::type)_decoder.decodeI32());
                _decoder.decodeFieldBegin();  // name str
                element.__set_name(_decoder.decodeString());
                if (element._type != Type::BOOLEAN) {
                    _decoder.decodeFieldBegin();  // conv type i32
                    element.__set_converted_type((ConvertedType::type)_decoder.decodeI32());
                    _decoder.decodeFieldBegin();  // logical struct
                    _decoder.decodeFieldBegin();  // logical type struct.
                    LogicalType ltype;

                    if (element._type == Type::INT64) {
                        IntType t;
                        _decoder.decodeFieldBegin();  // bitWidth byte
                        t.__set_bitWidth(_decoder.decodeByte());
                        //_decoder.decodeFieldBegin(); // isSigned bool
                        t.__set_isSigned(_decoder.decodeBool());
                        _decoder.decodeFieldStop();
                        ltype.__set_INTEGER(ltype.INTEGER);
                    } else if (element._type == Type::BYTE_ARRAY || element._type == Type::FIXED_LEN_BYTE_ARRAY) {
                        _decoder.decodeFieldStop();  // logical type stop
                        ltype.__set_STRING(ltype.STRING);
                    }
                    _decoder.decodeFieldStop();  // logical stop
                    element.__set_logical_type(ltype);
                }
                _decoder.decodeFieldStop();  // schema stop
                _schemas.push_back(element);
            }
            _fileMetaData.__set_schema(_schemas);
            _decoder.decodeFieldBegin();
            _numRows = _decoder.decodeI64();
            _fileMetaData.__set_num_rows(_numRows);

            _decoder.decodeFieldBegin();      // rowGroup struct
            _decoder.decodeListBegin(count);  // rowGroups list count
            std::vector<RowGroup> rowGroups;
            for (int i = 0; i < count; ++i) {
                RowGroup group;
                _decoder.decodeFieldBegin();  // ColumnChunk struct
                int ccCount = 0;
                _decoder.decodeListBegin(ccCount);  // ColumnChunk count
                std::vector<ColumnChunk> chunks;
                for (int j = 0; j < ccCount; ++j) {
                    ColumnChunk chunk;
                    _decoder.decodeFieldBegin();  // file_offset
                    chunk.__set_file_offset(_decoder.decodeI64());

                    _decoder.decodeFieldBegin();  // meta data
                    ColumnMetaData data;
                    _decoder.decodeFieldBegin();  // type
                    data.__set_type((Type::type)_decoder.decodeI32());
                    _decoder.decodeFieldBegin();  // encodings
                    int inCount = 0;
                    _decoder.decodeListBegin(inCount);
                    std::vector<Encoding::type> encodings;
                    encodings.reserve(inCount);
                    for (int k = 0; k < inCount; ++k) {
                        encodings.push_back((Encoding::type)_decoder.decodeI32());
                    }
                    data.__set_encodings(encodings);

                    _decoder.decodeFieldBegin();  // path in schema
                    _decoder.decodeListBegin(inCount);
                    std::vector<std::string> pathInSchema;
                    pathInSchema.reserve(inCount);
                    for (int k = 0; k < inCount; ++k) {
                        pathInSchema.push_back(_decoder.decodeString());
                    }
                    data.__set_path_in_schema(pathInSchema);

                    _decoder.decodeFieldBegin();  // codec
                    data.__set_codec((CompressionCodec::type)_decoder.decodeI32());
                    _decoder.decodeFieldBegin();  // num values
                    data.__set_num_values(_decoder.decodeI64());
                    _decoder.decodeFieldBegin();  // total uncompressed
                    data.__set_total_uncompressed_size(_decoder.decodeI64());
                    _decoder.decodeFieldBegin();  // total compressed
                    data.__set_total_compressed_size(_decoder.decodeI64());
                    _decoder.decodeFieldBegin();  // data page offset
                    data.__set_data_page_offset(_decoder.decodeI64());
                    _decoder.decodeFieldBegin();  // encoding stats
                    _decoder.decodeListBegin(inCount);
                    std::vector<PageEncodingStats> encodingStats;
                    for (int k = 0; k < inCount; ++k) {
                        PageEncodingStats stats;
                        _decoder.decodeFieldBegin();  // page_type
                        stats.__set_page_type((PageType::type)_decoder.decodeI32());
                        _decoder.decodeFieldBegin();  // encoding
                        stats.__set_encoding((Encoding::type)_decoder.decodeI32());
                        _decoder.decodeFieldBegin();  // count
                        stats.__set_count(_decoder.decodeI32());
                        _decoder.decodeFieldStop();  // encoding stop
                        encodingStats.push_back(stats);
                    }
                    data.__set_encoding_stats(encodingStats);
                    _decoder.decodeFieldStop();  // chunk metadata
                    chunk.__set_meta_data(data);
                    _decoder.decodeFieldStop();  // column chunk
                    chunks.push_back(chunk);
                }
                group.__set_columns(chunks);
                _decoder.decodeFieldBegin();  // total bytes
                group.__set_total_byte_size(_decoder.decodeI64());
                _decoder.decodeFieldBegin();  // num rows
                group.__set_num_rows(_decoder.decodeI64());
                _decoder.decodeFieldBegin();  // file offset
                group.__set_file_offset(_decoder.decodeI64());
                _decoder.decodeFieldBegin();  // total comp
                group.__set_total_compressed_size(_decoder.decodeI64());
                _decoder.decodeFieldBegin();  // total ordinal
                group.__set_ordinal(_decoder.decodeI16());
                _decoder.decodeFieldStop();  // Row Group stop
                rowGroups.push_back(group);
            }
            _fileMetaData.__set_row_groups(rowGroups);
            _decoder.decodeFieldBegin();  // created by
            _fileMetaData.__set_created_by(_decoder.decodeString());
            _decoder.decodeFieldBegin();      // column orders
            _decoder.decodeListBegin(count);  // column orders count
            std::vector<ColumnOrder> orders;
            for (int i = 0; i < count; ++i) {
                ColumnOrder order;
                TypeDefinedOrder TDO;
                _decoder.decodeFieldBegin();  // type order
                _decoder.decodeFieldStop();   // TDO Stop
                order.__set_TYPE_ORDER(TDO);
                orders.push_back(order);
            }
            _fileMetaData.__set_column_orders(orders);
            _decoder.decodeFieldStop();  // Column Orders stop
            _decoder.decodeFieldStop();  // File meta data stop
        }
        ReadIntoBatch();
    }

private:
    void MagicCheck(const char* magicBytes) {
        if (memcmp(magicBytes, PARQUET_MAGIC, 4) != 0) {
            std::stringstream ss;
            ss << "File: " << _filename->filename() << " does not start with PAR1 magic bytes.";
            throw std::runtime_error(ss.str());
        }
    }

    PageHeader DecodePageHeader(size_t offset, size_t headerSize) {
        PageHeader header;
        DataPageHeader dHeader;
        _file.seekg(offset, std::ios::beg);
        _file.read(reinterpret_cast<char*>(_buffer), headerSize);
        _decoder.setBuffer(_buffer);
        _decoder.decodeFieldBegin();  // type
        header.__set_type((PageType::type)_decoder.decodeI32());
        _decoder.decodeFieldBegin();  // compressed size
        header.__set_uncompressed_page_size(_decoder.decodeI32());
        _decoder.decodeFieldBegin();  // uncompressed size
        header.__set_compressed_page_size(_decoder.decodeI32());
        _decoder.decodeFieldBegin();  // data header
        _decoder.decodeFieldBegin();  // num values
        dHeader.__set_num_values(_decoder.decodeI32());
        _decoder.decodeFieldBegin();  // def encoding
        dHeader.__set_definition_level_encoding((Encoding::type)_decoder.decodeI32());
        _decoder.decodeFieldBegin();  // rep encoding
        dHeader.__set_repetition_level_encoding((Encoding::type)_decoder.decodeI32());
        _decoder.decodeFieldBegin();  // stats
        _decoder.decodeFieldStop();   // stats
        _decoder.decodeFieldStop();   // data header
        _decoder.decodeFieldStop();   // header
        header.__set_data_page_header(dHeader);
        return header;
    }

    void ReadData(Type::type type, size_t numValues, size_t dataSize, size_t offset, std::shared_ptr<RecordBatch>& batch, std::string& name) {
        if (_currentBufferLen <= dataSize) {
            delete _buffer;
            _buffer = new char[dataSize];
            _currentBufferLen = dataSize;
        }
        _file.seekg(offset, std::ios::beg);
        _file.read(reinterpret_cast<char*>(_buffer), dataSize);
        batch->ParseBuffer(type, _buffer, numValues, name);
    }

    void ReadIntoBatch() {
        size_t indexer = 1;
        for (const auto& rowGroup : _fileMetaData._rowGroups) {
            auto batch = std::make_shared<RecordBatch>(_schemas);
            _records.push_back(batch);
            for (const auto& chunk : rowGroup._columns) {
                size_t chunkOffset = chunk._fileOffset;
                auto pheader = DecodePageHeader(chunk._metaData._dataPageOffset, 19);
                size_t numValues = pheader._dataPageHeader._numValues;
                size_t dataSize = pheader._compressedPageSize;
                size_t dataOffset = chunkOffset - dataSize;
                auto type = chunk._metaData._type;
                ReadData(type, numValues, dataSize, dataOffset, batch, _schemas[indexer++]._name);
            }
            indexer = 0;
        }
    }

    std::shared_ptr<FileInputStream> _filename;
    bool _opened;
    std::fstream _file;
    ParquetSchema _schema;
    std::vector<SchemaElement> _schemas;
    FileMetaData _fileMetaData;
    ParquetThriftDecoder _decoder;
    int16_t _i16;
    int32_t _i32;
    int64_t _i64;
    int64_t _numRows;
    std::vector<std::shared_ptr<RecordBatch>> _records;
    char* _buffer;
    size_t _currentBufferLen;
};

}  // end namespace clearParquet
