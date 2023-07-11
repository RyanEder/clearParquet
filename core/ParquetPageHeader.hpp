#pragma once
#include "ParquetTypes.hpp"

namespace clearParquet {

// Aux for PageHeader
class Statistics {
public:
    Statistics() : _max(), _min(), _nullCount(0), _distinctCount(0), _maxValue(), _minValue() {}

    virtual ~Statistics() {}
    std::string _max;
    std::string _min;
    int64_t _nullCount;
    int64_t _distinctCount;
    std::string _maxValue;
    std::string _minValue;

    _Statistics__isset __isset;

    void __set_max(const std::string& val) {
        _max = val;
        __isset._max = true;
    }

    void __set_min(const std::string& val) {
        _min = val;
        __isset._min = true;
    }

    void __set_null_count(const int64_t val) {
        _nullCount = val;
        __isset._nullCount = true;
    }

    void __set_distinct_count(const int64_t val) {
        _distinctCount = val;
        __isset._distinctCount = true;
    }

    void __set_max_value(const std::string& val) {
        _maxValue = val;
        __isset._maxValue = true;
    }

    void __set_min_value(const std::string& val) {
        _minValue = val;
        __isset._minValue = true;
    }

    bool operator==(const Statistics& rhs) const {
        if (__isset._max != rhs.__isset._max)
            return false;
        else if (__isset._max && !(_max == rhs._max))
            return false;
        if (__isset._min != rhs.__isset._min)
            return false;
        else if (__isset._min && !(_min == rhs._min))
            return false;
        if (__isset._nullCount != rhs.__isset._nullCount)
            return false;
        else if (__isset._nullCount && !(_nullCount == rhs._nullCount))
            return false;
        if (__isset._distinctCount != rhs.__isset._distinctCount)
            return false;
        else if (__isset._distinctCount && !(_distinctCount == rhs._distinctCount))
            return false;
        if (__isset._maxValue != rhs.__isset._maxValue)
            return false;
        else if (__isset._maxValue && !(_maxValue == rhs._maxValue))
            return false;
        if (__isset._minValue != rhs.__isset._minValue)
            return false;
        else if (__isset._minValue && !(_minValue == rhs._minValue))
            return false;
        return true;
    }
    bool operator!=(const Statistics& rhs) const {
        return !(*this == rhs);
    }
};
class IndexPageHeader {
public:
    IndexPageHeader() {}

    virtual ~IndexPageHeader() {}
    bool operator==(const IndexPageHeader& /* rhs */) const {
        return true;
    }
    bool operator!=(const IndexPageHeader& rhs) const {
        return !(*this == rhs);
    }
};

class DataPageHeader {
public:
    DataPageHeader() noexcept
        : _numValues(0),
          _encoding(static_cast<Encoding::type>(0)),
          _definitionLevelEncoding(static_cast<Encoding::type>(0)),
          _repetitionLevelEncoding(static_cast<Encoding::type>(0)) {}

    virtual ~DataPageHeader() {}
    int32_t _numValues;
    Encoding::type _encoding;
    Encoding::type _definitionLevelEncoding;
    Encoding::type _repetitionLevelEncoding;
    Statistics _statistics;

    _DataPageHeader__isset __isset;

    void __set_num_values(const int32_t val) {
        _numValues = val;
    }

    void __set_encoding(const Encoding::type val) {
        _encoding = val;
    }

    void __set_definition_level_encoding(const Encoding::type val) {
        _definitionLevelEncoding = val;
    }

    void __set_repetition_level_encoding(const Encoding::type val) {
        _repetitionLevelEncoding = val;
    }

    void __set_statistics(const Statistics& val) {
        _statistics = val;
        __isset._statistics = true;
    }

    bool operator==(const DataPageHeader& rhs) const {
        if (!(_numValues == rhs._numValues))
            return false;
        if (!(_encoding == rhs._encoding))
            return false;
        if (!(_definitionLevelEncoding == rhs._definitionLevelEncoding))
            return false;
        if (!(_repetitionLevelEncoding == rhs._repetitionLevelEncoding))
            return false;
        if (__isset._statistics != rhs.__isset._statistics)
            return false;
        else if (__isset._statistics && !(_statistics == rhs._statistics))
            return false;
        return true;
    }
    bool operator!=(const DataPageHeader& rhs) const {
        return !(*this == rhs);
    }
};

class DictionaryPageHeader {
public:
    DictionaryPageHeader() : _numValues(0), _encoding(static_cast<Encoding::type>(0)), _isSorted(0) {}

    virtual ~DictionaryPageHeader() {}
    int32_t _numValues;
    Encoding::type _encoding;
    bool _isSorted;

    _DictionaryPageHeader__isset __isset;

    void __set_num_values(const int32_t val) {
        _numValues = val;
    }

    void __set_encoding(const Encoding::type val) {
        _encoding = val;
    }

    void __set_is_sorted(const bool val) {
        _isSorted = val;
        __isset._isSorted = true;
    }

    bool operator==(const DictionaryPageHeader& rhs) const {
        if (!(_numValues == rhs._numValues))
            return false;
        if (!(_encoding == rhs._encoding))
            return false;
        if (__isset._isSorted != rhs.__isset._isSorted)
            return false;
        else if (__isset._isSorted && !(_isSorted == rhs._isSorted))
            return false;
        return true;
    }
    bool operator!=(const DictionaryPageHeader& rhs) const {
        return !(*this == rhs);
    }
};

class DataPageHeaderV2 {
public:
    DataPageHeaderV2()
        : _numValues(0),
          _numNulls(0),
          _numRows(0),
          _encoding(static_cast<Encoding::type>(0)),
          _definitionLevelsByteLength(0),
          _repetitionLevelsByteLength(0),
          _isCompressed(true) {}

    virtual ~DataPageHeaderV2() {}
    int32_t _numValues;
    int32_t _numNulls;
    int32_t _numRows;
    Encoding::type _encoding;
    int32_t _definitionLevelsByteLength;
    int32_t _repetitionLevelsByteLength;
    bool _isCompressed;
    Statistics _statistics;

    _DataPageHeaderV2__isset __isset;

    void __set_num_values(const int32_t val) {
        _numValues = val;
    }

    void __set_num_nulls(const int32_t val) {
        _numNulls = val;
    }

    void __set_num_rows(const int32_t val) {
        _numRows = val;
    }

    void __set_encoding(const Encoding::type val) {
        _encoding = val;
    }

    void __set_definition_levels_byte_length(const int32_t val) {
        _definitionLevelsByteLength = val;
    }

    void __set_repetition_levels_byte_length(const int32_t val) {
        _repetitionLevelsByteLength = val;
    }

    void __set_is_compressed(const bool val) {
        _isCompressed = val;
        __isset._isCompressed = true;
    }

    void __set_statistics(const Statistics& val) {
        _statistics = val;
        __isset._statistics = true;
    }

    bool operator==(const DataPageHeaderV2& rhs) const {
        if (!(_numValues == rhs._numValues))
            return false;
        if (!(_numNulls == rhs._numNulls))
            return false;
        if (!(_numRows == rhs._numRows))
            return false;
        if (!(_encoding == rhs._encoding))
            return false;
        if (!(_definitionLevelsByteLength == rhs._definitionLevelsByteLength))
            return false;
        if (!(_repetitionLevelsByteLength == rhs._repetitionLevelsByteLength))
            return false;
        if (__isset._isCompressed != rhs.__isset._isCompressed)
            return false;
        else if (__isset._isCompressed && !(_isCompressed == rhs._isCompressed))
            return false;
        if (__isset._statistics != rhs.__isset._statistics)
            return false;
        else if (__isset._statistics && !(_statistics == rhs._statistics))
            return false;
        return true;
    }
    bool operator!=(const DataPageHeaderV2& rhs) const {
        return !(*this == rhs);
    }
};

class PageHeader {
public:
    PageHeader() : _type(static_cast<PageType::type>(0)), _uncompressedPageSize(0), _compressedPageSize(0), _crc(0) {}

    virtual ~PageHeader() {}
    PageType::type _type;
    int32_t _uncompressedPageSize;
    int32_t _compressedPageSize;
    int32_t _crc;
    DataPageHeader _dataPageHeader;
    IndexPageHeader _indexPageHeader;
    DictionaryPageHeader _dictionaryPageHeader;
    DataPageHeaderV2 _dataPageHeaderV2;

    _PageHeader__isset __isset;

    void __set_type(const PageType::type val) {
        _type = val;
    }

    void __set_uncompressed_page_size(const int32_t val) {
        _uncompressedPageSize = val;
    }

    void __set_compressed_page_size(const int32_t val) {
        _compressedPageSize = val;
    }

    void __set_crc(const int32_t val) {
        _crc = val;
        __isset._crc = true;
    }

    void __set_data_page_header(const DataPageHeader& val) {
        _dataPageHeader = val;
        __isset._dataPageHeader = true;
    }

    void __set_index_page_header(const IndexPageHeader& val) {
        _indexPageHeader = val;
        __isset._indexPageHeader = true;
    }

    void __set_dictionary_page_header(const DictionaryPageHeader& val) {
        _dictionaryPageHeader = val;
        __isset._dictionaryPageHeader = true;
    }

    void __set_data_page_header_v2(const DataPageHeaderV2& val) {
        _dataPageHeaderV2 = val;
        __isset._dataPageHeaderV2 = true;
    }

    bool operator==(const PageHeader& rhs) const {
        if (!(_type == rhs._type))
            return false;
        if (!(_uncompressedPageSize == rhs._uncompressedPageSize))
            return false;
        if (!(_compressedPageSize == rhs._compressedPageSize))
            return false;
        if (__isset._crc != rhs.__isset._crc)
            return false;
        else if (__isset._crc && !(_crc == rhs._crc))
            return false;
        if (__isset._dataPageHeader != rhs.__isset._dataPageHeader)
            return false;
        else if (__isset._dataPageHeader && !(_dataPageHeader == rhs._dataPageHeader))
            return false;
        if (__isset._indexPageHeader != rhs.__isset._indexPageHeader)
            return false;
        else if (__isset._indexPageHeader && !(_indexPageHeader == rhs._indexPageHeader))
            return false;
        if (__isset._dictionaryPageHeader != rhs.__isset._dictionaryPageHeader)
            return false;
        else if (__isset._dictionaryPageHeader && !(_dictionaryPageHeader == rhs._dictionaryPageHeader))
            return false;
        if (__isset._dataPageHeaderV2 != rhs.__isset._dataPageHeaderV2)
            return false;
        else if (__isset._dataPageHeaderV2 && !(_dataPageHeaderV2 == rhs._dataPageHeaderV2))
            return false;
        return true;
    }
    bool operator!=(const PageHeader& rhs) const {
        return !(*this == rhs);
    }
};
}  // end namespace clearParquet
