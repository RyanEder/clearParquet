#pragma once

#include <thrift/TApplicationException.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "ParquetColumnChunk.hpp"
#include "ParquetPageHeader.hpp"
#include "ParquetSchema.hpp"

//#define DEBUG 1
namespace clearParquet {
using ThriftBuffer = apache::thrift::transport::TMemoryBuffer;
class ThriftSerializer {
   public:
    ThriftSerializer() : _memBuffer(new ThriftBuffer(1024)) {
        _protocol = _factory.getProtocol(_memBuffer);
    }

    template <class T>
    void SerializeToBuffer(const T* obj, uint32_t* len, uint8_t** buffer) {
        _memBuffer->resetBuffer();
        SerializeObject(obj);
        _memBuffer->getBuffer(buffer, len);
    }

   private:
    template <class T>
    uint32_t SerializeObject(const T* obj) {
        std::cout << "Default Object serialize" << std::endl;
        return 0;
    }

    uint32_t SerializeObject(const Statistics* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("Statistics");

        if (obj->__isset._max) {
            xfer += oprot->writeFieldBegin("max", ::apache::thrift::protocol::T_STRING, 1);
            xfer += oprot->writeBinary(obj->_max);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._min) {
            xfer += oprot->writeFieldBegin("min", ::apache::thrift::protocol::T_STRING, 2);
            xfer += oprot->writeBinary(obj->_min);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._nullCount) {
            xfer += oprot->writeFieldBegin("null_count", ::apache::thrift::protocol::T_I64, 3);
            xfer += oprot->writeI64(obj->_nullCount);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._distinctCount) {
            xfer += oprot->writeFieldBegin("distinct_count", ::apache::thrift::protocol::T_I64, 4);
            xfer += oprot->writeI64(obj->_distinctCount);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._maxValue) {
            xfer += oprot->writeFieldBegin("max_value", ::apache::thrift::protocol::T_STRING, 5);
            xfer += oprot->writeBinary(obj->_maxValue);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._minValue) {
            xfer += oprot->writeFieldBegin("min_value", ::apache::thrift::protocol::T_STRING, 6);
            xfer += oprot->writeBinary(obj->_minValue);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    // Page Serializers
    uint32_t SerializeObject(const DataPageHeader* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("DataPageHeader");

        xfer += oprot->writeFieldBegin("num_values", ::apache::thrift::protocol::T_I32, 1);
        xfer += oprot->writeI32(obj->_numValues);
        xfer += oprot->writeFieldEnd();
#ifdef DEBUG
        std::cout << "DataPageHeader->NUM_VALUES: " << obj->_numValues << std::endl;
        std::cout << "DataPageHeader->ENCODING : " << obj->_encoding << std::endl;
        std::cout << "DataPageHeader->DEFENCODING : " << obj->_definitionLevelEncoding << std::endl;
        std::cout << "DataPageHeader->REPENCODING : " << obj->_repetitionLevelEncoding << std::endl;
#endif

        xfer += oprot->writeFieldBegin("encoding", ::apache::thrift::protocol::T_I32, 2);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_encoding));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("definition_level_encoding", ::apache::thrift::protocol::T_I32, 3);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_definitionLevelEncoding));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("repetition_level_encoding", ::apache::thrift::protocol::T_I32, 4);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_repetitionLevelEncoding));
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._statistics) {
#ifdef DEBUG
            std::cout << "DataPageHeader->STATISTICS: " << std::endl;
#endif
            xfer += oprot->writeFieldBegin("statistics", ::apache::thrift::protocol::T_STRUCT, 5);
            xfer += SerializeObject(&obj->_statistics);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();

        return xfer;
    }

    uint32_t SerializeObject(const PageHeader* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("PageHeader");
        xfer += oprot->writeFieldBegin("type", ::apache::thrift::protocol::T_I32, 1);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_type));
        xfer += oprot->writeFieldEnd();
#ifdef DEBUG
        std::cout << "PageHeader->TYPE: " << obj->_type << std::endl;
        std::cout << "PageHeader->UNCOMPRESSED_PAGE_SIZE: " << obj->_uncompressedPageSize << std::endl;
        std::cout << "PageHeader->COMPRESSED_PAGE_SIZE: " << obj->_compressedPageSize << std::endl;
#endif

        xfer += oprot->writeFieldBegin("uncompressed_page_size", ::apache::thrift::protocol::T_I32, 2);
        xfer += oprot->writeI32(obj->_uncompressedPageSize);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("compressed_page_size", ::apache::thrift::protocol::T_I32, 3);
        xfer += oprot->writeI32(obj->_compressedPageSize);
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._crc) {
            xfer += oprot->writeFieldBegin("crc", ::apache::thrift::protocol::T_I32, 4);
            xfer += oprot->writeI32(obj->_crc);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._dataPageHeader) {
            xfer += oprot->writeFieldBegin("data_page_header", ::apache::thrift::protocol::T_STRUCT, 5);
#ifdef DEBUG
            std::cout << "PageHeader->DATA_PAGE_HEADER: " << std::endl;
#endif
            xfer += SerializeObject(&obj->_dataPageHeader);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._indexPageHeader) {
            xfer += oprot->writeFieldBegin("index_page_header", ::apache::thrift::protocol::T_STRUCT, 6);
            xfer += SerializeObject(&obj->_indexPageHeader);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._dictionaryPageHeader) {
            xfer += oprot->writeFieldBegin("dictionary_page_header", ::apache::thrift::protocol::T_STRUCT, 7);
            xfer += SerializeObject(&obj->_dictionaryPageHeader);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._dataPageHeaderV2) {
            xfer += oprot->writeFieldBegin("data_page_header_v2", ::apache::thrift::protocol::T_STRUCT, 8);
            xfer += SerializeObject(&obj->_dataPageHeaderV2);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    // Column Serializers
    uint32_t SerializeObject(const PageEncodingStats* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();

#ifdef DEBUG
        std::cout << "ColumnChunk->PAGE_TYPE: " << obj->_pageType << std::endl;
        std::cout << "ColumnChunk->ENCODING: " << obj->_encoding << std::endl;
        std::cout << "ColumnChunk->COUNT: " << obj->_count << std::endl;
#endif
        xfer += oprot->writeStructBegin("PageEncodingStats");

        xfer += oprot->writeFieldBegin("page_type", ::apache::thrift::protocol::T_I32, 1);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_pageType));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("encoding", ::apache::thrift::protocol::T_I32, 2);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_encoding));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("count", ::apache::thrift::protocol::T_I32, 3);
        xfer += oprot->writeI32(obj->_count);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();

        return xfer;
    }

    uint32_t SerializeObject(const ColumnMetaData* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("ColumnMetaData");
#ifdef DEBUG
        std::cout << "ColumnChunk->TYPE: " << obj->_type << std::endl;
        std::cout << "ColumnChunk->CODEC: " << obj->_codec << std::endl;
        std::cout << "ColumnChunk->NUM_VALUES: " << obj->_numValues << std::endl;
        std::cout << "ColumnChunk->TOTUNCOM: " << obj->_totalUncompressedSize << std::endl;
        std::cout << "ColumnChunk->TOTCOM: " << obj->_totalCompressedSize << std::endl;
        std::cout << "ColumnChunk->DATA_PAGE_OFFSET: " << obj->_dataPageOffset << std::endl;
#endif

        xfer += oprot->writeFieldBegin("type", ::apache::thrift::protocol::T_I32, 1);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_type));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("encodings", ::apache::thrift::protocol::T_LIST, 2);
        {
            xfer += oprot->writeListBegin(::apache::thrift::protocol::T_I32, static_cast<uint32_t>(obj->_encodings.size()));
            std::vector<Encoding::type>::const_iterator _iter104;
            for (_iter104 = obj->_encodings.begin(); _iter104 != obj->_encodings.end(); ++_iter104) {
#ifdef DEBUG
                std::cout << "ColumnChunk->ENCODING: " << *_iter104 << std::endl;
#endif
                xfer += oprot->writeI32(static_cast<int32_t>((*_iter104)));
            }
            xfer += oprot->writeListEnd();
        }
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("path_in_schema", ::apache::thrift::protocol::T_LIST, 3);
        {
            xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRING, static_cast<uint32_t>(obj->_pathInSchema.size()));
            std::vector<std::string>::const_iterator _iter105;
            for (_iter105 = obj->_pathInSchema.begin(); _iter105 != obj->_pathInSchema.end(); ++_iter105) {
#ifdef DEBUG
                std::cout << "ColumnChunk->PATH_IN_SCHEMA: " << *_iter105 << std::endl;
#endif
                xfer += oprot->writeString((*_iter105));
            }
            xfer += oprot->writeListEnd();
        }
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("codec", ::apache::thrift::protocol::T_I32, 4);
        xfer += oprot->writeI32(static_cast<int32_t>(obj->_codec));
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("num_values", ::apache::thrift::protocol::T_I64, 5);
        xfer += oprot->writeI64(obj->_numValues);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("total_uncompressed_size", ::apache::thrift::protocol::T_I64, 6);
        xfer += oprot->writeI64(obj->_totalUncompressedSize);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("total_compressed_size", ::apache::thrift::protocol::T_I64, 7);
        xfer += oprot->writeI64(obj->_totalCompressedSize);
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._keyValueMetadata) {
            xfer += oprot->writeFieldBegin("key_value_metadata", ::apache::thrift::protocol::T_LIST, 8);
            {
                xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_keyValueMetadata.size()));
                std::vector<KeyValue>::const_iterator _iter106;
                for (_iter106 = obj->_keyValueMetadata.begin(); _iter106 != obj->_keyValueMetadata.end(); ++_iter106) {
                    xfer += SerializeObject(&(*_iter106));
                }
                xfer += oprot->writeListEnd();
            }
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldBegin("data_page_offset", ::apache::thrift::protocol::T_I64, 9);
        xfer += oprot->writeI64(obj->_dataPageOffset);
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._indexPageOffset) {
            xfer += oprot->writeFieldBegin("index_page_offset", ::apache::thrift::protocol::T_I64, 10);
            xfer += oprot->writeI64(obj->_indexPageOffset);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._dictionaryPageOffset) {
            xfer += oprot->writeFieldBegin("dictionary_page_offset", ::apache::thrift::protocol::T_I64, 11);
            xfer += oprot->writeI64(obj->_dictionaryPageOffset);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._statistics) {
            xfer += oprot->writeFieldBegin("statistics", ::apache::thrift::protocol::T_STRUCT, 12);
            xfer += SerializeObject(&obj->_statistics);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._encodingStats) {
            xfer += oprot->writeFieldBegin("encoding_stats", ::apache::thrift::protocol::T_LIST, 13);
            {
                xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_encodingStats.size()));
                std::vector<PageEncodingStats>::const_iterator _iter107;
                for (_iter107 = obj->_encodingStats.begin(); _iter107 != obj->_encodingStats.end(); ++_iter107) {
#ifdef DEBUG
                    std::cout << "ColumnChunk->PAGEENCODINGSTATS: " << std::endl;
#endif
                    xfer += SerializeObject(&(*_iter107));
                }
                xfer += oprot->writeListEnd();
            }
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._bloomFilterOffset) {
            xfer += oprot->writeFieldBegin("bloom_filter_offset", ::apache::thrift::protocol::T_I64, 14);
            xfer += oprot->writeI64(obj->_bloomFilterOffset);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();

        return xfer;
    }
    uint32_t SerializeObject(const ColumnChunk* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
#ifdef DEBUG
        std::cout << "ColumnChunk->FILE_OFFSET: " << obj->_fileOffset << std::endl;
#endif

        xfer += oprot->writeStructBegin("ColumnChunk");

        if (obj->__isset._filePath) {
            xfer += oprot->writeFieldBegin("file_path", ::apache::thrift::protocol::T_STRING, 1);
            xfer += oprot->writeString(obj->_filePath);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldBegin("file_offset", ::apache::thrift::protocol::T_I64, 2);
        xfer += oprot->writeI64(obj->_fileOffset);
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._metaData) {
#ifdef DEBUG
            std::cout << "ColumnChunk->META_DATA: " << std::endl;
#endif
            xfer += oprot->writeFieldBegin("meta_data", ::apache::thrift::protocol::T_STRUCT, 3);
            xfer += SerializeObject(&obj->_metaData);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._offsetIndexOffset) {
            xfer += oprot->writeFieldBegin("offset_index_offset", ::apache::thrift::protocol::T_I64, 4);
            xfer += oprot->writeI64(obj->_offsetIndexOffset);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._offsetIndexLength) {
            xfer += oprot->writeFieldBegin("offset_index_length", ::apache::thrift::protocol::T_I32, 5);
            xfer += oprot->writeI32(obj->_offsetIndexLength);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._columnIndexOffset) {
            xfer += oprot->writeFieldBegin("column_index_offset", ::apache::thrift::protocol::T_I64, 6);
            xfer += oprot->writeI64(obj->_columnIndexOffset);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._columnIndexLength) {
            xfer += oprot->writeFieldBegin("column_index_length", ::apache::thrift::protocol::T_I32, 7);
            xfer += oprot->writeI32(obj->_columnIndexLength);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._cryptoMetadata) {
            xfer += oprot->writeFieldBegin("crypto_metadata", ::apache::thrift::protocol::T_STRUCT, 8);
            xfer += SerializeObject(&obj->_cryptoMetadata);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._encryptedColumnMetadata) {
            xfer += oprot->writeFieldBegin("encrypted_column_metadata", ::apache::thrift::protocol::T_STRING, 9);
            xfer += oprot->writeBinary(obj->_encryptedColumnMetadata);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    uint32_t SerializeObject(const MilliSeconds* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("MilliSeconds");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const MicroSeconds* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("MicroSeconds");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const NanoSeconds* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("NanoSeconds");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const TimeUnit* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("TimeUnit");

        if (obj->__isset.MILLIS) {
            xfer += oprot->writeFieldBegin("MILLIS", ::apache::thrift::protocol::T_STRUCT, 1);
            xfer += SerializeObject(&obj->MILLIS);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.MICROS) {
            xfer += oprot->writeFieldBegin("MICROS", ::apache::thrift::protocol::T_STRUCT, 2);
            xfer += SerializeObject(&obj->MICROS);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.NANOS) {
            xfer += oprot->writeFieldBegin("NANOS", ::apache::thrift::protocol::T_STRUCT, 3);
            xfer += SerializeObject(&obj->NANOS);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const StringType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("StringType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const MapType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("MapType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const ListType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("ListType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const EnumType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("EnumType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const DecimalType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("DecimalType");
        xfer += oprot->writeFieldBegin("scale", ::apache::thrift::protocol::T_I32, 1);
        xfer += oprot->writeI32(obj->_scale);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("precision", ::apache::thrift::protocol::T_I32, 2);
        xfer += oprot->writeI32(obj->_precision);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const DateType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("DateType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const TimeType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("TimeType");
        xfer += oprot->writeFieldBegin("isAdjustedToUTC", ::apache::thrift::protocol::T_BOOL, 1);
        xfer += oprot->writeBool(obj->_isAdjustedToUTC);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("unit", ::apache::thrift::protocol::T_STRUCT, 2);
        xfer += SerializeObject(&obj->_unit);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const TimestampType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("TimestampType");
        xfer += oprot->writeFieldBegin("isAdjustedToUTC", ::apache::thrift::protocol::T_BOOL, 1);
        xfer += oprot->writeBool(obj->_isAdjustedToUTC);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("unit", ::apache::thrift::protocol::T_STRUCT, 2);
        xfer += SerializeObject(&obj->_unit);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const IntType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("IntType");
        xfer += oprot->writeFieldBegin("bitWidth", ::apache::thrift::protocol::T_BYTE, 1);
        xfer += oprot->writeByte(obj->_bitWidth);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("isSigned", ::apache::thrift::protocol::T_BOOL, 2);
        xfer += oprot->writeBool(obj->_isSigned);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const NullType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("NullType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const JsonType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("JsonType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const BsonType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("BsonType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const UUIDType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("UUIDType");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }
    uint32_t SerializeObject(const LogicalType* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("LogicalType");

        if (obj->__isset.STRING) {
#ifdef DEBUG
            std::cout << "FileMetaData->STRING: " << std::endl;
#endif
            xfer += oprot->writeFieldBegin("STRING", ::apache::thrift::protocol::T_STRUCT, 1);
            xfer += SerializeObject(&obj->STRING);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.MAP) {
            xfer += oprot->writeFieldBegin("MAP", ::apache::thrift::protocol::T_STRUCT, 2);
            xfer += SerializeObject(&obj->MAP);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.LIST) {
            xfer += oprot->writeFieldBegin("LIST", ::apache::thrift::protocol::T_STRUCT, 3);
            xfer += SerializeObject(&obj->LIST);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.ENUM) {
            xfer += oprot->writeFieldBegin("ENUM", ::apache::thrift::protocol::T_STRUCT, 4);
            xfer += SerializeObject(&obj->ENUM);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.DECIMAL) {
            xfer += oprot->writeFieldBegin("DECIMAL", ::apache::thrift::protocol::T_STRUCT, 5);
            xfer += SerializeObject(&obj->DECIMAL);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.DATE) {
            xfer += oprot->writeFieldBegin("DATE", ::apache::thrift::protocol::T_STRUCT, 6);
            xfer += SerializeObject(&obj->DATE);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.TIME) {
            xfer += oprot->writeFieldBegin("TIME", ::apache::thrift::protocol::T_STRUCT, 7);
            xfer += SerializeObject(&obj->TIME);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.TIMESTAMP) {
            xfer += oprot->writeFieldBegin("TIMESTAMP", ::apache::thrift::protocol::T_STRUCT, 8);
            xfer += SerializeObject(&obj->TIMESTAMP);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.INTEGER) {
#ifdef DEBUG
            std::cout << "FileMetaData->INT: " << std::endl;
#endif
            xfer += oprot->writeFieldBegin("INTEGER", ::apache::thrift::protocol::T_STRUCT, 10);
            xfer += SerializeObject(&obj->INTEGER);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.UNKNOWN) {
            xfer += oprot->writeFieldBegin("UNKNOWN", ::apache::thrift::protocol::T_STRUCT, 11);
            xfer += SerializeObject(&obj->UNKNOWN);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.JSON) {
            xfer += oprot->writeFieldBegin("JSON", ::apache::thrift::protocol::T_STRUCT, 12);
            xfer += SerializeObject(&obj->JSON);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.BSON) {
            xfer += oprot->writeFieldBegin("BSON", ::apache::thrift::protocol::T_STRUCT, 13);
            xfer += SerializeObject(&obj->BSON);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset.UUID) {
            xfer += oprot->writeFieldBegin("UUID", ::apache::thrift::protocol::T_STRUCT, 14);
            xfer += SerializeObject(&obj->UUID);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();

        return xfer;
    }
    uint32_t SerializeObject(const SchemaElement* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("SchemaElement");
#ifdef DEBUG
        std::cout << "FileMetaData->NAME: " << obj->_name << std::endl;
#endif
        if (obj->__isset._type) {
#ifdef DEBUG
            std::cout << "FileMetaData->TYPE: " << obj->_type << std::endl;
#endif
            xfer += oprot->writeFieldBegin("type", ::apache::thrift::protocol::T_I32, 1);
            xfer += oprot->writeI32(static_cast<int32_t>(obj->_type));
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._typeLength) {
            xfer += oprot->writeFieldBegin("type_length", ::apache::thrift::protocol::T_I32, 2);
            xfer += oprot->writeI32(obj->_typeLength);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._repetitionType) {
#ifdef DEBUG
            std::cout << "FileMetaData->REPTYPE: " << obj->_repetitionType << std::endl;
#endif
            xfer += oprot->writeFieldBegin("repetition_type", ::apache::thrift::protocol::T_I32, 3);
            xfer += oprot->writeI32(static_cast<int32_t>(obj->_repetitionType));
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldBegin("name", ::apache::thrift::protocol::T_STRING, 4);
        xfer += oprot->writeString(obj->_name);
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._numChildren) {
#ifdef DEBUG
            std::cout << "FileMetaData->NUM_CHILD: " << obj->_numChildren << std::endl;
#endif
            xfer += oprot->writeFieldBegin("num_children", ::apache::thrift::protocol::T_I32, 5);
            xfer += oprot->writeI32(obj->_numChildren);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._convertedType) {
#ifdef DEBUG
            std::cout << "FileMetaData->CONVTYPE: " << obj->_convertedType << std::endl;
#endif
            xfer += oprot->writeFieldBegin("converted_type", ::apache::thrift::protocol::T_I32, 6);
            xfer += oprot->writeI32(static_cast<int32_t>(obj->_convertedType));
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._scale) {
            xfer += oprot->writeFieldBegin("scale", ::apache::thrift::protocol::T_I32, 7);
            xfer += oprot->writeI32(obj->_scale);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._precision) {
            xfer += oprot->writeFieldBegin("precision", ::apache::thrift::protocol::T_I32, 8);
            xfer += oprot->writeI32(obj->_precision);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._fieldId) {
            xfer += oprot->writeFieldBegin("field_id", ::apache::thrift::protocol::T_I32, 9);
            xfer += oprot->writeI32(obj->_fieldId);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._logicalType) {
#ifdef DEBUG
            std::cout << "FileMetaData->LOGICALTYPE: " << std::endl;
#endif
            xfer += oprot->writeFieldBegin("logicalType", ::apache::thrift::protocol::T_STRUCT, 10);
            xfer += SerializeObject(&obj->_logicalType);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();

        return xfer;
    }

    uint32_t SerializeObject(const TypeDefinedOrder* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("TypeDefinedOrder");

        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    uint32_t SerializeObject(const ColumnOrder* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("ColumnOrder");

        if (obj->__isset.TYPE_ORDER) {
#ifdef DEBUG
            std::cout << "FileMetaData->TYPE_ORDER: " << std::endl;
#endif
            xfer += oprot->writeFieldBegin("TYPE_ORDER", ::apache::thrift::protocol::T_STRUCT, 1);
            xfer += SerializeObject(&obj->TYPE_ORDER);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    uint32_t SerializeObject(const RowGroup* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("RowGroup");
#ifdef DEBUG
        std::cout << "FileMetaData->TOTAL_BYTE_SIZE: " << obj->_totalByteSize << std::endl;
        std::cout << "FileMetaData->NUM_ROWS: " << obj->_numRows << std::endl;
        std::cout << "FileMetaData->FILE_OFFSET: " << obj->_fileOffset << std::endl;
        std::cout << "FileMetaData->TOTAL_COM_SIZE: " << obj->_totalCompressedSize << std::endl;
        std::cout << "FileMetaData->ORDINAL: " << obj->_ordinal << std::endl;
#endif

        xfer += oprot->writeFieldBegin("columns", ::apache::thrift::protocol::T_LIST, 1);
        {
            xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_columns.size()));
            std::vector<ColumnChunk>::const_iterator _iter134;
            for (_iter134 = obj->_columns.begin(); _iter134 != obj->_columns.end(); ++_iter134) {
#ifdef DEBUG
                std::cout << "FileMetaData->COLUMN: " << std::endl;
#endif
                xfer += SerializeObject(&(*_iter134));
            }
            xfer += oprot->writeListEnd();
        }
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("total_byte_size", ::apache::thrift::protocol::T_I64, 2);
        xfer += oprot->writeI64(obj->_totalByteSize);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("num_rows", ::apache::thrift::protocol::T_I64, 3);
        xfer += oprot->writeI64(obj->_numRows);
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._sortingColumns) {
            xfer += oprot->writeFieldBegin("sorting_columns", ::apache::thrift::protocol::T_LIST, 4);
            {
                xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_sortingColumns.size()));
                std::vector<SortingColumn>::const_iterator _iter135;
                for (_iter135 = obj->_sortingColumns.begin(); _iter135 != obj->_sortingColumns.end(); ++_iter135) {
                    xfer += SerializeObject(&(*_iter135));
                }
                xfer += oprot->writeListEnd();
            }
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._fileOffset) {
            xfer += oprot->writeFieldBegin("file_offset", ::apache::thrift::protocol::T_I64, 5);
            xfer += oprot->writeI64(obj->_fileOffset);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._totalCompressedSize) {
            xfer += oprot->writeFieldBegin("total_compressed_size", ::apache::thrift::protocol::T_I64, 6);
            xfer += oprot->writeI64(obj->_totalCompressedSize);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._ordinal) {
            xfer += oprot->writeFieldBegin("ordinal", ::apache::thrift::protocol::T_I16, 7);
            xfer += oprot->writeI16(obj->_ordinal);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    uint32_t SerializeObject(const FileMetaData* obj) {
        uint32_t xfer = 0;
        auto oprot = _protocol.get();
        xfer += oprot->writeStructBegin("FileMetaData");
#ifdef DEBUG
        std::cout << "FileMetaData->VERSION: " << obj->_version << std::endl;
        std::cout << "FileMetaData->NUM_ROWS: " << obj->_numRows << std::endl;
        std::cout << "FileMetaData->CREATED: " << obj->_createdBy << std::endl;
#endif

        xfer += oprot->writeFieldBegin("version", ::apache::thrift::protocol::T_I32, 1);
        xfer += oprot->writeI32(obj->_version);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("schema", ::apache::thrift::protocol::T_LIST, 2);
        {
            xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_schema.size()));
            std::vector<SchemaElement>::const_iterator _iter205;
            for (_iter205 = obj->_schema.begin(); _iter205 != obj->_schema.end(); ++_iter205) {
#ifdef DEBUG
                std::cout << "FileMetaData->SCHEMA: " << std::endl;
#endif
                xfer += SerializeObject(&(*_iter205));
            }
            xfer += oprot->writeListEnd();
        }
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("num_rows", ::apache::thrift::protocol::T_I64, 3);
        xfer += oprot->writeI64(obj->_numRows);
        xfer += oprot->writeFieldEnd();

        xfer += oprot->writeFieldBegin("row_groups", ::apache::thrift::protocol::T_LIST, 4);
        {
            xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_rowGroups.size()));
            std::vector<RowGroup>::const_iterator _iter206;
            for (_iter206 = obj->_rowGroups.begin(); _iter206 != obj->_rowGroups.end(); ++_iter206) {
#ifdef DEBUG
                std::cout << "FileMetaData->ROW_GROUPS: " << std::endl;
#endif
                xfer += SerializeObject(&(*_iter206));
            }
            xfer += oprot->writeListEnd();
        }
        xfer += oprot->writeFieldEnd();

        if (obj->__isset._keyValueMetadata) {
            xfer += oprot->writeFieldBegin("key_value_metadata", ::apache::thrift::protocol::T_LIST, 5);
            {
                xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_keyValueMetadata.size()));
                std::vector<KeyValue>::const_iterator _iter207;
                for (_iter207 = obj->_keyValueMetadata.begin(); _iter207 != obj->_keyValueMetadata.end(); ++_iter207) {
                    xfer += SerializeObject(&(*_iter207));
                }
                xfer += oprot->writeListEnd();
            }
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._createdBy) {
            xfer += oprot->writeFieldBegin("created_by", ::apache::thrift::protocol::T_STRING, 6);
            xfer += oprot->writeString(obj->_createdBy);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._columnOrders) {
            xfer += oprot->writeFieldBegin("column_orders", ::apache::thrift::protocol::T_LIST, 7);
            {
                xfer += oprot->writeListBegin(::apache::thrift::protocol::T_STRUCT, static_cast<uint32_t>(obj->_columnOrders.size()));
                std::vector<ColumnOrder>::const_iterator _iter208;
                for (_iter208 = obj->_columnOrders.begin(); _iter208 != obj->_columnOrders.end(); ++_iter208) {
#ifdef DEBUG
                    std::cout << "FileMetaData->COLUMNORDERS: " << std::endl;
#endif
                    xfer += SerializeObject(&(*_iter208));
                }
                xfer += oprot->writeListEnd();
            }
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._encryptionAlgorithm) {
            xfer += oprot->writeFieldBegin("encryption_algorithm", ::apache::thrift::protocol::T_STRUCT, 8);
            xfer += SerializeObject(&obj->_encryptionAlgorithm);
            xfer += oprot->writeFieldEnd();
        }
        if (obj->__isset._footerSigningKeyMetadata) {
            xfer += oprot->writeFieldBegin("footer_signing_key_metadata", ::apache::thrift::protocol::T_STRING, 9);
            xfer += oprot->writeBinary(obj->_footerSigningKeyMetadata);
            xfer += oprot->writeFieldEnd();
        }
        xfer += oprot->writeFieldStop();
        xfer += oprot->writeStructEnd();
        return xfer;
    }

    apache::thrift::protocol::TCompactProtocolFactoryT<ThriftBuffer> _factory;
    std::shared_ptr<ThriftBuffer> _memBuffer;
    std::shared_ptr<apache::thrift::protocol::TProtocol> _protocol;
};

}  // end namespace clearParquet
