#pragma once

#include "ParquetColumnChunk.hpp"
#include "ParquetPageHeader.hpp"
#include "ParquetSchema.hpp"
#include "ParquetThriftEncoder.hpp"

#include <cstring>

//#define DEBUG 1
namespace clearParquet {

// Constants
constexpr size_t FIELD_IDENTIFIER_1 = 1;
constexpr size_t FIELD_IDENTIFIER_2 = 2;
constexpr size_t FIELD_IDENTIFIER_3 = 3;
constexpr size_t FIELD_IDENTIFIER_4 = 4;
constexpr size_t FIELD_IDENTIFIER_5 = 5;
constexpr size_t FIELD_IDENTIFIER_6 = 6;
constexpr size_t FIELD_IDENTIFIER_7 = 7;
constexpr size_t FIELD_IDENTIFIER_8 = 8;
constexpr size_t FIELD_IDENTIFIER_9 = 9;
constexpr size_t FIELD_IDENTIFIER_10 = 10;
constexpr size_t FIELD_IDENTIFIER_11 = 11;
constexpr size_t FIELD_IDENTIFIER_12 = 12;
constexpr size_t FIELD_IDENTIFIER_13 = 13;
constexpr size_t FIELD_IDENTIFIER_14 = 14;
constexpr size_t THRIFT_BUFFER_SIZE = 1024;

class ThriftSerializer {
public:
    ThriftSerializer() {}

    template <class T>
    void SerializeToBuffer(const T* obj, uint32_t* len, uint8_t** buffer) {
        _encoder.clearBuffer();
        SerializeObject(obj);
        if (buffer != nullptr && *buffer == nullptr) {
            *len = _encoder.getBufferSize();
            *buffer = new uint8_t[*len];
            memcpy(*buffer, _encoder.getBufferData(), *len);
        }
    }

private:
    template <class T>
    void SerializeObject(const T* obj) {
        // std::cout << "Default Object serialize" << std::endl;
    }

    void SerializeObject(const Statistics* obj) {
        _encoder.writeStructBegin("Statistics");

        if (obj->__isset._max) {
            _encoder.writeFieldBegin("max", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_1);
            _encoder.writeBinary(obj->_max);
        }
        if (obj->__isset._min) {
            _encoder.writeFieldBegin("min", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_2);
            _encoder.writeBinary(obj->_min);
        }
        if (obj->__isset._nullCount) {
            _encoder.writeFieldBegin("null_count", ThriftFieldType::T_I64, FIELD_IDENTIFIER_3);
            _encoder.writeI64(obj->_nullCount);
        }
        if (obj->__isset._distinctCount) {
            _encoder.writeFieldBegin("distinct_count", ThriftFieldType::T_I64, FIELD_IDENTIFIER_4);
            _encoder.writeI64(obj->_distinctCount);
        }
        if (obj->__isset._maxValue) {
            _encoder.writeFieldBegin("max_value", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_5);
            _encoder.writeBinary(obj->_maxValue);
        }
        if (obj->__isset._minValue) {
            _encoder.writeFieldBegin("min_value", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_6);
            _encoder.writeBinary(obj->_minValue);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    // Page Serializers
    void SerializeObject(const DataPageHeader* obj) {
        _encoder.writeStructBegin("DataPageHeader");

        _encoder.writeFieldBegin("num_values", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
        _encoder.writeI32(obj->_numValues);
#ifdef DEBUG
        std::cout << "DataPageHeader->NUM_VALUES: " << obj->_numValues << std::endl;
        std::cout << "DataPageHeader->ENCODING : " << obj->_encoding << std::endl;
        std::cout << "DataPageHeader->DEFENCODING : " << obj->_definitionLevelEncoding << std::endl;
        std::cout << "DataPageHeader->REPENCODING : " << obj->_repetitionLevelEncoding << std::endl;
#endif

        _encoder.writeFieldBegin("encoding", ThriftFieldType::T_I32, FIELD_IDENTIFIER_2);
        _encoder.writeI32(static_cast<int32_t>(obj->_encoding));

        _encoder.writeFieldBegin("definition_level_encoding", ThriftFieldType::T_I32, FIELD_IDENTIFIER_3);
        _encoder.writeI32(static_cast<int32_t>(obj->_definitionLevelEncoding));

        _encoder.writeFieldBegin("repetition_level_encoding", ThriftFieldType::T_I32, FIELD_IDENTIFIER_4);
        _encoder.writeI32(static_cast<int32_t>(obj->_repetitionLevelEncoding));

        if (obj->__isset._statistics) {
#ifdef DEBUG
            std::cout << "DataPageHeader->STATISTICS: " << std::endl;
#endif
            _encoder.writeFieldBegin("statistics", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_5);
            SerializeObject(&obj->_statistics);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const PageHeader* obj) {
        _encoder.writeStructBegin("PageHeader");
        _encoder.writeFieldBegin("type", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
        _encoder.writeI32(static_cast<int32_t>(obj->_type));
#ifdef DEBUG
        std::cout << "PageHeader->TYPE: " << obj->_type << std::endl;
        std::cout << "PageHeader->UNCOMPRESSED_PAGE_SIZE: " << obj->_uncompressedPageSize << std::endl;
        std::cout << "PageHeader->COMPRESSED_PAGE_SIZE: " << obj->_compressedPageSize << std::endl;
#endif

        _encoder.writeFieldBegin("uncompressed_page_size", ThriftFieldType::T_I32, FIELD_IDENTIFIER_2);
        _encoder.writeI32(obj->_uncompressedPageSize);

        _encoder.writeFieldBegin("compressed_page_size", ThriftFieldType::T_I32, FIELD_IDENTIFIER_3);
        _encoder.writeI32(obj->_compressedPageSize);

        if (obj->__isset._crc) {
            _encoder.writeFieldBegin("crc", ThriftFieldType::T_I32, FIELD_IDENTIFIER_4);
            _encoder.writeI32(obj->_crc);
        }
        if (obj->__isset._dataPageHeader) {
            _encoder.writeFieldBegin("data_page_header", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_5);
#ifdef DEBUG
            std::cout << "PageHeader->DATA_PAGE_HEADER: " << std::endl;
#endif
            SerializeObject(&obj->_dataPageHeader);
        }
        if (obj->__isset._indexPageHeader) {
            _encoder.writeFieldBegin("index_page_header", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_6);
            SerializeObject(&obj->_indexPageHeader);
        }
        if (obj->__isset._dictionaryPageHeader) {
            _encoder.writeFieldBegin("dictionary_page_header", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_7);
            SerializeObject(&obj->_dictionaryPageHeader);
        }
        if (obj->__isset._dataPageHeaderV2) {
            _encoder.writeFieldBegin("data_page_header_v2", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_8);
            SerializeObject(&obj->_dataPageHeaderV2);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    // Column Serializers
    void SerializeObject(const PageEncodingStats* obj) {
#ifdef DEBUG
        std::cout << "ColumnChunk->PAGE_TYPE: " << obj->_pageType << std::endl;
        std::cout << "ColumnChunk->ENCODING: " << obj->_encoding << std::endl;
        std::cout << "ColumnChunk->COUNT: " << obj->_count << std::endl;
#endif
        _encoder.writeStructBegin("PageEncodingStats");

        _encoder.writeFieldBegin("page_type", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
        _encoder.writeI32(static_cast<int32_t>(obj->_pageType));

        _encoder.writeFieldBegin("encoding", ThriftFieldType::T_I32, FIELD_IDENTIFIER_2);
        _encoder.writeI32(static_cast<int32_t>(obj->_encoding));

        _encoder.writeFieldBegin("count", ThriftFieldType::T_I32, FIELD_IDENTIFIER_3);
        _encoder.writeI32(obj->_count);

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const ColumnMetaData* obj) {
        _encoder.writeStructBegin("ColumnMetaData");
#ifdef DEBUG
        std::cout << "ColumnChunk->TYPE: " << obj->_type << std::endl;
        std::cout << "ColumnChunk->CODEC: " << obj->_codec << std::endl;
        std::cout << "ColumnChunk->NUM_VALUES: " << obj->_numValues << std::endl;
        std::cout << "ColumnChunk->TOTUNCOM: " << obj->_totalUncompressedSize << std::endl;
        std::cout << "ColumnChunk->TOTCOM: " << obj->_totalCompressedSize << std::endl;
        std::cout << "ColumnChunk->DATA_PAGE_OFFSET: " << obj->_dataPageOffset << std::endl;
#endif

        _encoder.writeFieldBegin("type", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
        _encoder.writeI32(static_cast<int32_t>(obj->_type));

        _encoder.writeFieldBegin("encodings", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_2);
        {
            _encoder.writeListBegin(ThriftFieldType::T_I32, static_cast<uint32_t>(obj->_encodings.size()));
            std::vector<Encoding::type>::const_iterator _iter104;
            for (_iter104 = obj->_encodings.begin(); _iter104 != obj->_encodings.end(); ++_iter104) {
#ifdef DEBUG
                std::cout << "ColumnChunk->ENCODING: " << *_iter104 << std::endl;
#endif
                _encoder.writeI32(static_cast<int32_t>((*_iter104)));
            }
            _encoder.writeListEnd();
        }

        _encoder.writeFieldBegin("path_in_schema", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_3);
        {
            _encoder.writeListBegin(ThriftFieldType::T_STRING, static_cast<uint32_t>(obj->_pathInSchema.size()));
            std::vector<std::string>::const_iterator _iter105;
            for (_iter105 = obj->_pathInSchema.begin(); _iter105 != obj->_pathInSchema.end(); ++_iter105) {
#ifdef DEBUG
                std::cout << "ColumnChunk->PATH_IN_SCHEMA: " << *_iter105 << std::endl;
#endif
                _encoder.writeString((*_iter105));
            }
            _encoder.writeListEnd();
        }

        _encoder.writeFieldBegin("codec", ThriftFieldType::T_I32, FIELD_IDENTIFIER_4);
        _encoder.writeI32(static_cast<int32_t>(obj->_codec));

        _encoder.writeFieldBegin("num_values", ThriftFieldType::T_I64, FIELD_IDENTIFIER_5);
        _encoder.writeI64(obj->_numValues);

        _encoder.writeFieldBegin("total_uncompressed_size", ThriftFieldType::T_I64, FIELD_IDENTIFIER_6);
        _encoder.writeI64(obj->_totalUncompressedSize);

        _encoder.writeFieldBegin("total_compressed_size", ThriftFieldType::T_I64, FIELD_IDENTIFIER_7);
        _encoder.writeI64(obj->_totalCompressedSize);

        if (obj->__isset._keyValueMetadata) {
            _encoder.writeFieldBegin("key_value_metadata", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_8);
            {
                _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_keyValueMetadata.size()));
                std::vector<KeyValue>::const_iterator _iter106;
                for (_iter106 = obj->_keyValueMetadata.begin(); _iter106 != obj->_keyValueMetadata.end(); ++_iter106) {
                    SerializeObject(&(*_iter106));
                }
                _encoder.writeListEnd();
            }
        }
        _encoder.writeFieldBegin("data_page_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_9);
        _encoder.writeI64(obj->_dataPageOffset);

        if (obj->__isset._indexPageOffset) {
            _encoder.writeFieldBegin("index_page_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_10);
            _encoder.writeI64(obj->_indexPageOffset);
        }
        if (obj->__isset._dictionaryPageOffset) {
            _encoder.writeFieldBegin("dictionary_page_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_11);
            _encoder.writeI64(obj->_dictionaryPageOffset);
        }
        if (obj->__isset._statistics) {
            _encoder.writeFieldBegin("statistics", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_12);
            SerializeObject(&obj->_statistics);
        }
        if (obj->__isset._encodingStats) {
            _encoder.writeFieldBegin("encoding_stats", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_13);
            {
                _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_encodingStats.size()));
                std::vector<PageEncodingStats>::const_iterator _iter107;
                for (_iter107 = obj->_encodingStats.begin(); _iter107 != obj->_encodingStats.end(); ++_iter107) {
#ifdef DEBUG
                    std::cout << "ColumnChunk->PAGEENCODINGSTATS: " << std::endl;
#endif
                    SerializeObject(&(*_iter107));
                }
                _encoder.writeListEnd();
            }
        }
        if (obj->__isset._bloomFilterOffset) {
            _encoder.writeFieldBegin("bloom_filter_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_14);
            _encoder.writeI64(obj->_bloomFilterOffset);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const ColumnChunk* obj) {
#ifdef DEBUG
        std::cout << "ColumnChunk->FILE_OFFSET: " << obj->_fileOffset << std::endl;
#endif

        _encoder.writeStructBegin("ColumnChunk");

        if (obj->__isset._filePath) {
            _encoder.writeFieldBegin("file_path", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_1);
            _encoder.writeString(obj->_filePath);
        }
        _encoder.writeFieldBegin("file_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_2);
        _encoder.writeI64(obj->_fileOffset);

        if (obj->__isset._metaData) {
#ifdef DEBUG
            std::cout << "ColumnChunk->META_DATA: " << std::endl;
#endif
            _encoder.writeFieldBegin("meta_data", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_3);
            SerializeObject(&obj->_metaData);
        }
        if (obj->__isset._offsetIndexOffset) {
            _encoder.writeFieldBegin("offset_index_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_4);
            _encoder.writeI64(obj->_offsetIndexOffset);
        }
        if (obj->__isset._offsetIndexLength) {
            _encoder.writeFieldBegin("offset_index_length", ThriftFieldType::T_I32, FIELD_IDENTIFIER_5);
            _encoder.writeI32(obj->_offsetIndexLength);
        }
        if (obj->__isset._columnIndexOffset) {
            _encoder.writeFieldBegin("column_index_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_6);
            _encoder.writeI64(obj->_columnIndexOffset);
        }
        if (obj->__isset._columnIndexLength) {
            _encoder.writeFieldBegin("column_index_length", ThriftFieldType::T_I32, FIELD_IDENTIFIER_7);
            _encoder.writeI32(obj->_columnIndexLength);
        }
        if (obj->__isset._cryptoMetadata) {
            _encoder.writeFieldBegin("crypto_metadata", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_8);
            SerializeObject(&obj->_cryptoMetadata);
        }
        if (obj->__isset._encryptedColumnMetadata) {
            _encoder.writeFieldBegin("encrypted_column_metadata", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_9);
            _encoder.writeBinary(obj->_encryptedColumnMetadata);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const MilliSeconds* obj) {
        _encoder.writeStructBegin("MilliSeconds");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const MicroSeconds* obj) {
        _encoder.writeStructBegin("MicroSeconds");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const NanoSeconds* obj) {
        _encoder.writeStructBegin("NanoSeconds");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const TimeUnit* obj) {
        _encoder.writeStructBegin("TimeUnit");

        if (obj->__isset.MILLIS) {
            _encoder.writeFieldBegin("MILLIS", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_1);
            SerializeObject(&obj->MILLIS);
        }
        if (obj->__isset.MICROS) {
            _encoder.writeFieldBegin("MICROS", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_2);
            SerializeObject(&obj->MICROS);
        }
        if (obj->__isset.NANOS) {
            _encoder.writeFieldBegin("NANOS", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_3);
            SerializeObject(&obj->NANOS);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const StringType* obj) {
        _encoder.writeStructBegin("StringType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const MapType* obj) {
        _encoder.writeStructBegin("MapType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const ListType* obj) {
        _encoder.writeStructBegin("ListType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const EnumType* obj) {
        _encoder.writeStructBegin("EnumType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const DecimalType* obj) {
        _encoder.writeStructBegin("DecimalType");
        _encoder.writeFieldBegin("scale", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
        _encoder.writeI32(obj->_scale);

        _encoder.writeFieldBegin("precision", ThriftFieldType::T_I32, FIELD_IDENTIFIER_2);
        _encoder.writeI32(obj->_precision);

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const DateType* obj) {
        _encoder.writeStructBegin("DateType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const TimeType* obj) {
        _encoder.writeStructBegin("TimeType");
        _encoder.writeFieldBegin("isAdjustedToUTC", ThriftFieldType::T_BOOL, FIELD_IDENTIFIER_1);
        _encoder.writeBool(obj->_isAdjustedToUTC);

        _encoder.writeFieldBegin("unit", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_2);
        SerializeObject(&obj->_unit);

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const TimestampType* obj) {
        _encoder.writeStructBegin("TimestampType");
        _encoder.writeFieldBegin("isAdjustedToUTC", ThriftFieldType::T_BOOL, FIELD_IDENTIFIER_1);
        _encoder.writeBool(obj->_isAdjustedToUTC);

        _encoder.writeFieldBegin("unit", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_2);
        SerializeObject(&obj->_unit);

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const IntType* obj) {
        _encoder.writeStructBegin("IntType");
        _encoder.writeFieldBegin("bitWidth", ThriftFieldType::T_BYTE, FIELD_IDENTIFIER_1);
        _encoder.writeByte(obj->_bitWidth);

        _encoder.writeFieldBegin("isSigned", ThriftFieldType::T_BOOL, FIELD_IDENTIFIER_2);
        _encoder.writeBool(obj->_isSigned);

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const NullType* obj) {
        _encoder.writeStructBegin("NullType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const JsonType* obj) {
        _encoder.writeStructBegin("JsonType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const BsonType* obj) {
        _encoder.writeStructBegin("BsonType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const UUIDType* obj) {
        _encoder.writeStructBegin("UUIDType");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const LogicalType* obj) {
        _encoder.writeStructBegin("LogicalType");

        if (obj->__isset.STRING) {
#ifdef DEBUG
            std::cout << "FileMetaData->STRING: " << std::endl;
#endif
            _encoder.writeFieldBegin("STRING", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_1);
            SerializeObject(&obj->STRING);
        }
        if (obj->__isset.MAP) {
            _encoder.writeFieldBegin("MAP", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_2);
            SerializeObject(&obj->MAP);
        }
        if (obj->__isset.LIST) {
            _encoder.writeFieldBegin("LIST", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_3);
            SerializeObject(&obj->LIST);
        }
        if (obj->__isset.ENUM) {
            _encoder.writeFieldBegin("ENUM", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_4);
            SerializeObject(&obj->ENUM);
        }
        if (obj->__isset.DECIMAL) {
            _encoder.writeFieldBegin("DECIMAL", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_5);
            SerializeObject(&obj->DECIMAL);
        }
        if (obj->__isset.DATE) {
            _encoder.writeFieldBegin("DATE", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_6);
            SerializeObject(&obj->DATE);
        }
        if (obj->__isset.TIME) {
            _encoder.writeFieldBegin("TIME", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_7);
            SerializeObject(&obj->TIME);
        }
        if (obj->__isset.TIMESTAMP) {
            _encoder.writeFieldBegin("TIMESTAMP", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_8);
            SerializeObject(&obj->TIMESTAMP);
        }
        if (obj->__isset.INTEGER) {
#ifdef DEBUG
            std::cout << "FileMetaData->INT: " << std::endl;
#endif
            _encoder.writeFieldBegin("INTEGER", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_10);
            SerializeObject(&obj->INTEGER);
        }
        if (obj->__isset.UNKNOWN) {
            _encoder.writeFieldBegin("UNKNOWN", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_11);
            SerializeObject(&obj->UNKNOWN);
        }
        if (obj->__isset.JSON) {
            _encoder.writeFieldBegin("JSON", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_12);
            SerializeObject(&obj->JSON);
        }
        if (obj->__isset.BSON) {
            _encoder.writeFieldBegin("BSON", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_13);
            SerializeObject(&obj->BSON);
        }
        if (obj->__isset.UUID) {
            _encoder.writeFieldBegin("UUID", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_14);
            SerializeObject(&obj->UUID);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }
    void SerializeObject(const SchemaElement* obj) {
        _encoder.writeStructBegin("SchemaElement");
#ifdef DEBUG
        std::cout << "FileMetaData->NAME: " << obj->_name << std::endl;
#endif
        if (obj->__isset._type) {
#ifdef DEBUG
            std::cout << "FileMetaData->TYPE: " << obj->_type << std::endl;
#endif
            _encoder.writeFieldBegin("type", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
            _encoder.writeI32(static_cast<int32_t>(obj->_type));
        }
        if (obj->__isset._typeLength) {
            _encoder.writeFieldBegin("type_length", ThriftFieldType::T_I32, FIELD_IDENTIFIER_2);
            _encoder.writeI32(obj->_typeLength);
        }
        if (obj->__isset._repetitionType) {
#ifdef DEBUG
            std::cout << "FileMetaData->REPTYPE: " << obj->_repetitionType << std::endl;
#endif
            _encoder.writeFieldBegin("repetition_type", ThriftFieldType::T_I32, FIELD_IDENTIFIER_3);
            _encoder.writeI32(static_cast<int32_t>(obj->_repetitionType));
        }
        _encoder.writeFieldBegin("name", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_4);
        _encoder.writeString(obj->_name);

        if (obj->__isset._numChildren) {
#ifdef DEBUG
            std::cout << "FileMetaData->NUM_CHILD: " << obj->_numChildren << std::endl;
#endif
            _encoder.writeFieldBegin("num_children", ThriftFieldType::T_I32, FIELD_IDENTIFIER_5);
            _encoder.writeI32(obj->_numChildren);
        }
        if (obj->__isset._convertedType) {
#ifdef DEBUG
            std::cout << "FileMetaData->CONVTYPE: " << obj->_convertedType << std::endl;
#endif
            _encoder.writeFieldBegin("converted_type", ThriftFieldType::T_I32, FIELD_IDENTIFIER_6);
            _encoder.writeI32(static_cast<int32_t>(obj->_convertedType));
        }
        if (obj->__isset._scale) {
            _encoder.writeFieldBegin("scale", ThriftFieldType::T_I32, FIELD_IDENTIFIER_7);
            _encoder.writeI32(obj->_scale);
        }
        if (obj->__isset._precision) {
            _encoder.writeFieldBegin("precision", ThriftFieldType::T_I32, FIELD_IDENTIFIER_8);
            _encoder.writeI32(obj->_precision);
        }
        if (obj->__isset._fieldId) {
            _encoder.writeFieldBegin("field_id", ThriftFieldType::T_I32, FIELD_IDENTIFIER_9);
            _encoder.writeI32(obj->_fieldId);
        }
        if (obj->__isset._logicalType) {
#ifdef DEBUG
            std::cout << "FileMetaData->LOGICALTYPE: " << std::endl;
#endif
            _encoder.writeFieldBegin("logicalType", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_10);
            SerializeObject(&obj->_logicalType);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const TypeDefinedOrder* obj) {
        _encoder.writeStructBegin("TypeDefinedOrder");

        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const ColumnOrder* obj) {
        _encoder.writeStructBegin("ColumnOrder");

        if (obj->__isset.TYPE_ORDER) {
#ifdef DEBUG
            std::cout << "FileMetaData->TYPE_ORDER: " << std::endl;
#endif
            _encoder.writeFieldBegin("TYPE_ORDER", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_1);
            SerializeObject(&obj->TYPE_ORDER);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const RowGroup* obj) {
        _encoder.writeStructBegin("RowGroup");
#ifdef DEBUG
        std::cout << "FileMetaData->TOTAL_BYTE_SIZE: " << obj->_totalByteSize << std::endl;
        std::cout << "FileMetaData->NUM_ROWS: " << obj->_numRows << std::endl;
        std::cout << "FileMetaData->FILE_OFFSET: " << obj->_fileOffset << std::endl;
        std::cout << "FileMetaData->TOTAL_COM_SIZE: " << obj->_totalCompressedSize << std::endl;
        std::cout << "FileMetaData->ORDINAL: " << obj->_ordinal << std::endl;
#endif

        _encoder.writeFieldBegin("columns", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_1);
        {
            _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_columns.size()));
            std::vector<ColumnChunk>::const_iterator _iter134;
            for (_iter134 = obj->_columns.begin(); _iter134 != obj->_columns.end(); ++_iter134) {
#ifdef DEBUG
                std::cout << "FileMetaData->COLUMN: " << std::endl;
#endif
                SerializeObject(&(*_iter134));
            }
            _encoder.writeListEnd();
        }

        _encoder.writeFieldBegin("total_byte_size", ThriftFieldType::T_I64, FIELD_IDENTIFIER_2);
        _encoder.writeI64(obj->_totalByteSize);

        _encoder.writeFieldBegin("num_rows", ThriftFieldType::T_I64, FIELD_IDENTIFIER_3);
        _encoder.writeI64(obj->_numRows);

        if (obj->__isset._sortingColumns) {
            _encoder.writeFieldBegin("sorting_columns", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_4);
            {
                _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_sortingColumns.size()));
                std::vector<SortingColumn>::const_iterator _iter135;
                for (_iter135 = obj->_sortingColumns.begin(); _iter135 != obj->_sortingColumns.end(); ++_iter135) {
                    SerializeObject(&(*_iter135));
                }
                _encoder.writeListEnd();
            }
        }
        if (obj->__isset._fileOffset) {
            _encoder.writeFieldBegin("file_offset", ThriftFieldType::T_I64, FIELD_IDENTIFIER_5);
            _encoder.writeI64(obj->_fileOffset);
        }
        if (obj->__isset._totalCompressedSize) {
            _encoder.writeFieldBegin("total_compressed_size", ThriftFieldType::T_I64, FIELD_IDENTIFIER_6);
            _encoder.writeI64(obj->_totalCompressedSize);
        }
        if (obj->__isset._ordinal) {
            _encoder.writeFieldBegin("ordinal", ThriftFieldType::T_I16, FIELD_IDENTIFIER_7);
            _encoder.writeI16(obj->_ordinal);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    void SerializeObject(const FileMetaData* obj) {
        _encoder.writeStructBegin("FileMetaData");
#ifdef DEBUG
        std::cout << "FileMetaData->VERSION: " << obj->_version << std::endl;
        std::cout << "FileMetaData->NUM_ROWS: " << obj->_numRows << std::endl;
        std::cout << "FileMetaData->CREATED: " << obj->_createdBy << std::endl;
#endif

        _encoder.writeFieldBegin("version", ThriftFieldType::T_I32, FIELD_IDENTIFIER_1);
        _encoder.writeI32(obj->_version);

        _encoder.writeFieldBegin("schema", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_2);
        {
            _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_schema.size()));
            std::vector<SchemaElement>::const_iterator _iter205;
            for (_iter205 = obj->_schema.begin(); _iter205 != obj->_schema.end(); ++_iter205) {
#ifdef DEBUG
                std::cout << "FileMetaData->SCHEMA: " << std::endl;
#endif
                SerializeObject(&(*_iter205));
            }
            _encoder.writeListEnd();
        }

        _encoder.writeFieldBegin("num_rows", ThriftFieldType::T_I64, FIELD_IDENTIFIER_3);
        _encoder.writeI64(obj->_numRows);

        _encoder.writeFieldBegin("row_groups", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_4);
        {
            _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_rowGroups.size()));
            std::vector<RowGroup>::const_iterator _iter206;
            for (_iter206 = obj->_rowGroups.begin(); _iter206 != obj->_rowGroups.end(); ++_iter206) {
#ifdef DEBUG
                std::cout << "FileMetaData->ROW_GROUPS: " << std::endl;
#endif
                SerializeObject(&(*_iter206));
            }
            _encoder.writeListEnd();
        }

        if (obj->__isset._keyValueMetadata) {
            _encoder.writeFieldBegin("key_value_metadata", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_5);
            {
                _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_keyValueMetadata.size()));
                std::vector<KeyValue>::const_iterator _iter207;
                for (_iter207 = obj->_keyValueMetadata.begin(); _iter207 != obj->_keyValueMetadata.end(); ++_iter207) {
                    SerializeObject(&(*_iter207));
                }
                _encoder.writeListEnd();
            }
        }
        if (obj->__isset._createdBy) {
            _encoder.writeFieldBegin("created_by", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_6);
            _encoder.writeString(obj->_createdBy);
        }
        if (obj->__isset._columnOrders) {
            _encoder.writeFieldBegin("column_orders", ThriftFieldType::T_LIST, FIELD_IDENTIFIER_7);
            {
                _encoder.writeListBegin(ThriftFieldType::T_STRUCT, static_cast<uint32_t>(obj->_columnOrders.size()));
                std::vector<ColumnOrder>::const_iterator _iter208;
                for (_iter208 = obj->_columnOrders.begin(); _iter208 != obj->_columnOrders.end(); ++_iter208) {
#ifdef DEBUG
                    std::cout << "FileMetaData->COLUMNORDERS: " << std::endl;
#endif
                    SerializeObject(&(*_iter208));
                }
                _encoder.writeListEnd();
            }
        }
        if (obj->__isset._encryptionAlgorithm) {
            _encoder.writeFieldBegin("encryption_algorithm", ThriftFieldType::T_STRUCT, FIELD_IDENTIFIER_8);
            SerializeObject(&obj->_encryptionAlgorithm);
        }
        if (obj->__isset._footerSigningKeyMetadata) {
            _encoder.writeFieldBegin("footer_signing_key_metadata", ThriftFieldType::T_STRING, FIELD_IDENTIFIER_9);
            _encoder.writeBinary(obj->_footerSigningKeyMetadata);
        }
        _encoder.writeFieldStop();
        _encoder.writeStructEnd();
    }

    ParquetThriftEncoder _encoder;
};

}  // end namespace clearParquet
