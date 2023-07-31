#pragma once

#include "ParquetPageHeader.hpp"
#include "ParquetTypes.hpp"

#include <iostream>
#include <string>
#include <vector>

namespace clearParquet {

// Aux for ColumnChunk
class KeyValue {
public:
    KeyValue() : _key(), _value() {}

    virtual ~KeyValue() {}
    std::string _key;
    std::string _value;

    _KeyValue__isset __isset;

    void __set_key(const std::string& val) {
        _key = val;
    }

    void __set_value(const std::string& val) {
        _value = val;
        __isset._value = true;
    }

    bool operator==(const KeyValue& rhs) const {
        if (!(_key == rhs._key))
            return false;
        if (__isset._value != rhs.__isset._value)
            return false;
        else if (__isset._value && !(_value == rhs._value))
            return false;
        return true;
    }
    bool operator!=(const KeyValue& rhs) const {
        return !(*this == rhs);
    }
};

class PageEncodingStats {
public:
    PageEncodingStats() : _pageType(static_cast<PageType::type>(0)), _encoding(static_cast<Encoding::type>(0)), _count(0) {}

    virtual ~PageEncodingStats() {}

    PageType::type _pageType;
    Encoding::type _encoding;
    int32_t _count;

    void __set_page_type(const PageType::type val) {
        _pageType = val;
    }

    void __set_encoding(const Encoding::type val) {
        _encoding = val;
    }

    void __set_count(const int32_t val) {
        _count = val;
    }

    bool operator==(const PageEncodingStats& rhs) const {
        if (!(_pageType == rhs._pageType))
            return false;
        if (!(_encoding == rhs._encoding))
            return false;
        if (!(_count == rhs._count))
            return false;
        return true;
    }
    bool operator!=(const PageEncodingStats& rhs) const {
        return !(*this == rhs);
    }
};

class ColumnMetaData {
public:
    ColumnMetaData()
        : _type(static_cast<Type::type>(0)),
          _codec(static_cast<CompressionCodec::type>(0)),
          _numValues(0),
          _totalUncompressedSize(0),
          _totalCompressedSize(0),
          _dataPageOffset(0),
          _indexPageOffset(0),
          _dictionaryPageOffset(0),
          _bloomFilterOffset(0) {}

    virtual ~ColumnMetaData() {}
    Type::type _type;
    std::vector<Encoding::type> _encodings;
    std::vector<std::string> _pathInSchema;
    CompressionCodec::type _codec;
    int64_t _numValues;
    int64_t _totalUncompressedSize;
    int64_t _totalCompressedSize;
    std::vector<KeyValue> _keyValueMetadata;
    int64_t _dataPageOffset;
    int64_t _indexPageOffset;
    int64_t _dictionaryPageOffset;
    Statistics _statistics;
    std::vector<PageEncodingStats> _encodingStats;
    int64_t _bloomFilterOffset;

    _ColumnMetaData__isset __isset;

    void __set_type(const Type::type val) {
        _type = val;
    }

    void __set_encodings(const std::vector<Encoding::type>& val) {
        _encodings = val;
    }

    void __set_path_in_schema(const std::vector<std::string>& val) {
        _pathInSchema = val;
    }

    void __set_codec(const CompressionCodec::type val) {
        _codec = val;
    }

    void __set_num_values(const int64_t val) {
        _numValues = val;
    }

    void __set_total_uncompressed_size(const int64_t val) {
        _totalUncompressedSize = val;
    }

    void __set_total_compressed_size(const int64_t val) {
        _totalCompressedSize = val;
    }

    void __set_key_value_metadata(const std::vector<KeyValue>& val) {
        _keyValueMetadata = val;
        __isset._keyValueMetadata = true;
    }

    void __set_data_page_offset(const int64_t val) {
        _dataPageOffset = val;
    }

    void __set_index_page_offset(const int64_t val) {
        _indexPageOffset = val;
        __isset._indexPageOffset = true;
    }

    void __set_dictionary_page_offset(const int64_t val) {
        _dictionaryPageOffset = val;
        __isset._dictionaryPageOffset = true;
    }

    void __set_statistics(const Statistics& val) {
        _statistics = val;
        __isset._statistics = true;
    }

    void __set_encoding_stats(const std::vector<PageEncodingStats>& val) {
        _encodingStats = val;
        __isset._encodingStats = true;
    }

    void __set_bloom_filter_offset(const int64_t val) {
        _bloomFilterOffset = val;
        __isset._bloomFilterOffset = true;
    }

    bool operator==(const ColumnMetaData& rhs) const {
        if (!(_type == rhs._type))
            return false;
        if (!(_encodings == rhs._encodings))
            return false;
        if (!(_pathInSchema == rhs._pathInSchema))
            return false;
        if (!(_codec == rhs._codec))
            return false;
        if (!(_numValues == rhs._numValues))
            return false;
        if (!(_totalUncompressedSize == rhs._totalUncompressedSize))
            return false;
        if (!(_totalCompressedSize == rhs._totalCompressedSize))
            return false;
        if (__isset._keyValueMetadata != rhs.__isset._keyValueMetadata)
            return false;
        else if (__isset._keyValueMetadata && !(_keyValueMetadata == rhs._keyValueMetadata))
            return false;
        if (!(_dataPageOffset == rhs._dataPageOffset))
            return false;
        if (__isset._indexPageOffset != rhs.__isset._indexPageOffset)
            return false;
        else if (__isset._indexPageOffset && !(_indexPageOffset == rhs._indexPageOffset))
            return false;
        if (__isset._dictionaryPageOffset != rhs.__isset._dictionaryPageOffset)
            return false;
        else if (__isset._dictionaryPageOffset && !(_dictionaryPageOffset == rhs._dictionaryPageOffset))
            return false;
        if (__isset._statistics != rhs.__isset._statistics)
            return false;
        else if (__isset._statistics && !(_statistics == rhs._statistics))
            return false;
        if (__isset._encodingStats != rhs.__isset._encodingStats)
            return false;
        else if (__isset._encodingStats && !(_encodingStats == rhs._encodingStats))
            return false;
        if (__isset._bloomFilterOffset != rhs.__isset._bloomFilterOffset)
            return false;
        else if (__isset._bloomFilterOffset && !(_bloomFilterOffset == rhs._bloomFilterOffset))
            return false;
        return true;
    }
    bool operator!=(const ColumnMetaData& rhs) const {
        return !(*this == rhs);
    }
};
std::ostream& operator<<(std::ostream& os, const ColumnMetaData& obj) {
    os << "   Type: " << obj._type << std::endl;
    os << "   Codec: " << obj._codec << std::endl;
    os << "   NumValues: " << obj._numValues << std::endl;
    os << "   TotalUncompressedSize: " << obj._totalUncompressedSize << std::endl;
    os << "   TotalCompressedSize: " << obj._totalCompressedSize << std::endl;
    os << "   DataPageOffset: " << obj._dataPageOffset << std::endl;
    return os;
}

class EncryptionWithFooterKey {
public:
    EncryptionWithFooterKey() {}
    virtual ~EncryptionWithFooterKey() {}

    bool operator==(const EncryptionWithFooterKey& /* rhs */) const {
        return true;
    }
    bool operator!=(const EncryptionWithFooterKey& rhs) const {
        return !(*this == rhs);
    }
};

class EncryptionWithColumnKey {
public:
    EncryptionWithColumnKey() : _keyMetadata() {}

    virtual ~EncryptionWithColumnKey() {}
    std::vector<std::string> _pathInSchema;
    std::string _keyMetadata;

    _EncryptionWithColumnKey__isset __isset;

    void __set_path_in_schema(const std::vector<std::string>& val) {
        _pathInSchema = val;
    }

    void __set_key_metadata(const std::string& val) {
        _keyMetadata = val;
        __isset._keyMetadata = true;
    }

    bool operator==(const EncryptionWithColumnKey& rhs) const {
        if (!(_pathInSchema == rhs._pathInSchema))
            return false;
        if (__isset._keyMetadata != rhs.__isset._keyMetadata)
            return false;
        else if (__isset._keyMetadata && !(_keyMetadata == rhs._keyMetadata))
            return false;
        return true;
    }
    bool operator!=(const EncryptionWithColumnKey& rhs) const {
        return !(*this == rhs);
    }
};

class ColumnCryptoMetaData {
public:
    ColumnCryptoMetaData() {}
    virtual ~ColumnCryptoMetaData() {}

    EncryptionWithFooterKey ENCRYPTION_WITH_FOOTER_KEY;
    EncryptionWithColumnKey ENCRYPTION_WITH_COLUMN_KEY;

    _ColumnCryptoMetaData__isset __isset;

    void __set_ENCRYPTION_WITH_FOOTER_KEY(const EncryptionWithFooterKey& val) {
        ENCRYPTION_WITH_FOOTER_KEY = val;
        __isset.ENCRYPTION_WITH_FOOTER_KEY = true;
    }

    void __set_ENCRYPTION_WITH_COLUMN_KEY(const EncryptionWithColumnKey& val) {
        ENCRYPTION_WITH_COLUMN_KEY = val;
        __isset.ENCRYPTION_WITH_COLUMN_KEY = true;
    }

    bool operator==(const ColumnCryptoMetaData& rhs) const {
        if (__isset.ENCRYPTION_WITH_FOOTER_KEY != rhs.__isset.ENCRYPTION_WITH_FOOTER_KEY)
            return false;
        else if (__isset.ENCRYPTION_WITH_FOOTER_KEY && !(ENCRYPTION_WITH_FOOTER_KEY == rhs.ENCRYPTION_WITH_FOOTER_KEY))
            return false;
        if (__isset.ENCRYPTION_WITH_COLUMN_KEY != rhs.__isset.ENCRYPTION_WITH_COLUMN_KEY)
            return false;
        else if (__isset.ENCRYPTION_WITH_COLUMN_KEY && !(ENCRYPTION_WITH_COLUMN_KEY == rhs.ENCRYPTION_WITH_COLUMN_KEY))
            return false;
        return true;
    }
    bool operator!=(const ColumnCryptoMetaData& rhs) const {
        return !(*this == rhs);
    }
};

class ColumnChunk {
public:
    ColumnChunk()
        : _filePath(), _fileOffset(0), _offsetIndexOffset(0), _offsetIndexLength(0), _columnIndexOffset(0), _columnIndexLength(0), _encryptedColumnMetadata() {}

    virtual ~ColumnChunk() {}
    std::string _filePath;
    int64_t _fileOffset;
    ColumnMetaData _metaData;
    int64_t _offsetIndexOffset;
    int32_t _offsetIndexLength;
    int64_t _columnIndexOffset;
    int32_t _columnIndexLength;
    ColumnCryptoMetaData _cryptoMetadata;
    std::string _encryptedColumnMetadata;

    _ColumnChunk__isset __isset;

    void __set_file_path(const std::string& val) {
        _filePath = val;
        __isset._filePath = true;
    }

    void __set_file_offset(const int64_t val) {
        _fileOffset = val;
    }

    void __set_meta_data(const ColumnMetaData& val) {
        _metaData = val;
        __isset._metaData = true;
    }

    void __set_offset_index_offset(const int64_t val) {
        _offsetIndexOffset = val;
        __isset._offsetIndexOffset = true;
    }

    void __set_offset_index_length(const int32_t val) {
        _offsetIndexLength = val;
        __isset._offsetIndexLength = true;
    }

    void __set_column_index_offset(const int64_t val) {
        _columnIndexOffset = val;
        __isset._columnIndexOffset = true;
    }

    void __set_column_index_length(const int32_t val) {
        _columnIndexLength = val;
        __isset._columnIndexLength = true;
    }

    void __set_crypto_metadata(const ColumnCryptoMetaData& val) {
        _cryptoMetadata = val;
        __isset._cryptoMetadata = true;
    }

    void __set_encrypted_column_metadata(const std::string& val) {
        _encryptedColumnMetadata = val;
        __isset._encryptedColumnMetadata = true;
    }

    bool operator==(const ColumnChunk& rhs) const {
        if (__isset._filePath != rhs.__isset._filePath)
            return false;
        else if (__isset._filePath && !(_filePath == rhs._filePath))
            return false;
        if (!(_fileOffset == rhs._fileOffset))
            return false;
        if (__isset._metaData != rhs.__isset._metaData)
            return false;
        else if (__isset._metaData && !(_metaData == rhs._metaData))
            return false;
        if (__isset._offsetIndexOffset != rhs.__isset._offsetIndexOffset)
            return false;
        else if (__isset._offsetIndexOffset && !(_offsetIndexOffset == rhs._offsetIndexOffset))
            return false;
        if (__isset._offsetIndexLength != rhs.__isset._offsetIndexLength)
            return false;
        else if (__isset._offsetIndexLength && !(_offsetIndexLength == rhs._offsetIndexLength))
            return false;
        if (__isset._columnIndexOffset != rhs.__isset._columnIndexOffset)
            return false;
        else if (__isset._columnIndexOffset && !(_columnIndexOffset == rhs._columnIndexOffset))
            return false;
        if (__isset._columnIndexLength != rhs.__isset._columnIndexLength)
            return false;
        else if (__isset._columnIndexLength && !(_columnIndexLength == rhs._columnIndexLength))
            return false;
        if (__isset._cryptoMetadata != rhs.__isset._cryptoMetadata)
            return false;
        else if (__isset._cryptoMetadata && !(_cryptoMetadata == rhs._cryptoMetadata))
            return false;
        if (__isset._encryptedColumnMetadata != rhs.__isset._encryptedColumnMetadata)
            return false;
        else if (__isset._encryptedColumnMetadata && !(_encryptedColumnMetadata == rhs._encryptedColumnMetadata))
            return false;
        return true;
    }
    bool operator!=(const ColumnChunk& rhs) const {
        return !(*this == rhs);
    }
};

std::ostream& operator<<(std::ostream& os, const ColumnChunk& obj) {
    os << "  FilePath: " << obj._filePath << std::endl;
    os << "  FileOffset: " << obj._fileOffset << std::endl;
    os << "  Metadata: \n" << obj._metaData << std::endl;
    os << "  OffsetIndexOffset: " << obj._offsetIndexOffset << std::endl;
    os << "  OffsetIndexLength: " << obj._offsetIndexLength << std::endl;
    os << "  ColumnIndexOffset: " << obj._columnIndexOffset << std::endl;
    os << "  ColumnIndexLength: " << obj._columnIndexLength << std::endl;
    return os;
}

}  // end namespace clearParquet
