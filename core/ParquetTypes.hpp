#pragma once
#include <string>

namespace clearParquet {

struct Compression {
    enum type { UNCOMPRESSED, SNAPPY, GZIP, BROTLI, ZSTD, LZ4, LZ4_FRAME, LZO, BZ2, LZ4_HADOOP };
};

struct Type {
    enum type { BOOLEAN = 0, INT32 = 1, INT64 = 2, INT96 = 3, FLOAT = 4, DOUBLE = 5, BYTE_ARRAY = 6, FIXED_LEN_BYTE_ARRAY = 7, NONE = 8 };
};

struct ConvertedType {
    enum type {
        UTF8 = 0,
        MAP = 1,
        MAP_KEY_VALUE = 2,
        LIST = 3,
        ENUM = 4,
        DECIMAL = 5,
        DATE = 6,
        TIME_MILLIS = 7,
        TIME_MICROS = 8,
        TIMESTAMP_MILLIS = 9,
        TIMESTAMP_MICROS = 10,
        UINT_8 = 11,
        UINT_16 = 12,
        UINT_32 = 13,
        UINT_64 = 14,
        INT_8 = 15,
        INT_16 = 16,
        INT_32 = 17,
        INT_64 = 18,
        JSON = 19,
        BSON = 20,
        INTERVAL = 21,
        NONE,
        NA,
        UNDEFINED

    };
};

struct Encoding {
    enum type {
        PLAIN = 0,
        PLAIN_DICTIONARY = 2,
        RLE = 3,
        BIT_PACKED = 4,
        DELTA_BINARY_PACKED = 5,
        DELTA_LENGTH_BYTE_ARRAY = 6,
        DELTA_BYTE_ARRAY = 7,
        RLE_DICTIONARY = 8,
        BYTE_STREAM_SPLIT = 9
    };
};

std::string ConvertedTypeToString(ConvertedType::type t) {
    switch (t) {
        case ConvertedType::NONE:
            return "NONE";
        case ConvertedType::UTF8:
            return "UTF8";
        case ConvertedType::MAP_KEY_VALUE:
            return "MAP_KEY_VALUE";
        case ConvertedType::LIST:
            return "LIST";
        case ConvertedType::ENUM:
            return "ENUM";
        case ConvertedType::DECIMAL:
            return "DECIMAL";
        case ConvertedType::DATE:
            return "DATE";
        case ConvertedType::TIME_MILLIS:
            return "TIME_MILLIS";
        case ConvertedType::TIME_MICROS:
            return "TIME_MICROS";
        case ConvertedType::TIMESTAMP_MILLIS:
            return "TIMESTAMP_MILLIS";
        case ConvertedType::TIMESTAMP_MICROS:
            return "TIMESTAMP_MICROS";
        case ConvertedType::UINT_8:
            return "UINT_8";
        case ConvertedType::UINT_16:
            return "UINT_16";
        case ConvertedType::UINT_32:
            return "UINT_32";
        case ConvertedType::UINT_64:
            return "UINT_64";
        case ConvertedType::INT_8:
            return "INT_8";
        case ConvertedType::INT_16:
            return "INT_16";
        case ConvertedType::INT_32:
            return "INT_32";
        case ConvertedType::INT_64:
            return "INT_64";
        case ConvertedType::JSON:
            return "JSON";
        case ConvertedType::BSON:
            return "BSON";
        case ConvertedType::INTERVAL:
            return "INTERVAL";
        default:
            return "UNKNOWN";
    }
}

struct Repetition {
    enum type { REQUIRED = 0, OPTIONAL = 1, REPEATED = 2 };
};
using FieldRepetitionType = Repetition;

// Compact Protocol field types
enum class ThriftFieldType : uint8_t {
    T_STOP = 0x00,
    T_BOOL = 0x01,
    T_BOOL_FALSE = 0x02,
    T_BYTE = 0x03,
    T_I16 = 0x04,
    T_I32 = 0x05,
    T_I64 = 0x06,
    T_DOUBLE = 0x07,
    T_STRING = 0x08,
    T_LIST = 0x09,
    T_SET = 0x0A,
    T_MAP = 0x0B,
    T_STRUCT = 0x0C
};

static constexpr char PARQUET_MAGIC[4] = {'P', 'A', 'R', '1'};

typedef struct _TimeUnit__isset {
    _TimeUnit__isset() : MILLIS(false), MICROS(false), NANOS(false) {}
    bool MILLIS : 1;
    bool MICROS : 1;
    bool NANOS : 1;
} _TimeUnit__isset;

class MilliSeconds {
public:
    MilliSeconds() {}
    virtual ~MilliSeconds() {}
    bool operator==(const MilliSeconds& /* rhs */) const {
        return true;
    }
    bool operator!=(const MilliSeconds& rhs) const {
        return !(*this == rhs);
    }
};
class MicroSeconds {
public:
    MicroSeconds() {}
    virtual ~MicroSeconds() {}
    bool operator==(const MicroSeconds& /* rhs */) const {
        return true;
    }
    bool operator!=(const MicroSeconds& rhs) const {
        return !(*this == rhs);
    }
};
class NanoSeconds {
public:
    NanoSeconds() {}
    virtual ~NanoSeconds() {}
    bool operator==(const NanoSeconds& /* rhs */) const {
        return true;
    }
    bool operator!=(const NanoSeconds& rhs) const {
        return !(*this == rhs);
    }
};

class TimeUnit {
public:
    TimeUnit() {}
    virtual ~TimeUnit() {}

    MilliSeconds MILLIS;
    MicroSeconds MICROS;
    NanoSeconds NANOS;
    _TimeUnit__isset __isset;
    void __set_MILLIS(const MilliSeconds& val) {
        MILLIS = val;
        __isset.MILLIS = true;
    }

    void __set_MICROS(const MicroSeconds& val) {
        MICROS = val;
        __isset.MICROS = true;
    }

    void __set_NANOS(const NanoSeconds& val) {
        NANOS = val;
        __isset.NANOS = true;
    }

    bool operator==(const TimeUnit& rhs) const {
        if (__isset.MILLIS != rhs.__isset.MILLIS)
            return false;
        else if (__isset.MILLIS && !(MILLIS == rhs.MILLIS))
            return false;
        if (__isset.MICROS != rhs.__isset.MICROS)
            return false;
        else if (__isset.MICROS && !(MICROS == rhs.MICROS))
            return false;
        if (__isset.NANOS != rhs.__isset.NANOS)
            return false;
        else if (__isset.NANOS && !(NANOS == rhs.NANOS))
            return false;
        return true;
    }
    bool operator!=(const TimeUnit& rhs) const {
        return !(*this == rhs);
    }
};

class StringType {
public:
    StringType() {}
    virtual ~StringType() {}
    bool operator==(const StringType& /* rhs */) const {
        return true;
    }
    bool operator!=(const StringType& rhs) const {
        return !(*this == rhs);
    }
};

class MapType {
public:
    MapType() {}
    virtual ~MapType() {}
    bool operator==(const MapType& /* rhs */) const {
        return true;
    }
    bool operator!=(const MapType& rhs) const {
        return !(*this == rhs);
    }
};

class ListType {
public:
    ListType() {}
    virtual ~ListType() {}
    bool operator==(const ListType& /* rhs */) const {
        return true;
    }
    bool operator!=(const ListType& rhs) const {
        return !(*this == rhs);
    }
};

class EnumType {
public:
    EnumType() {}
    virtual ~EnumType() {}
    bool operator==(const EnumType& /* rhs */) const {
        return true;
    }
    bool operator!=(const EnumType& rhs) const {
        return !(*this == rhs);
    }
};

class DecimalType {
public:
    DecimalType() : _scale(0), _precision(0) {}
    virtual ~DecimalType() {}

    int32_t _scale;
    int32_t _precision;

    void __set_scale(const int32_t val) {
        _scale = val;
    }

    void __set_precision(const int32_t val) {
        _precision = val;
    }

    bool operator==(const DecimalType& rhs) const {
        if (!(_scale == rhs._scale))
            return false;
        if (!(_precision == rhs._precision))
            return false;
        return true;
    }
    bool operator!=(const DecimalType& rhs) const {
        return !(*this == rhs);
    }
};

class DateType {
public:
    DateType() {}
    virtual ~DateType() {}
    bool operator==(const DateType& /* rhs */) const {
        return true;
    }
    bool operator!=(const DateType& rhs) const {
        return !(*this == rhs);
    }
};

class TimeType {
public:
    TimeType() : _isAdjustedToUTC(0) {}
    virtual ~TimeType() {}
    bool _isAdjustedToUTC;
    TimeUnit _unit;

    void __set_isAdjustedToUTC(const bool val) {
        _isAdjustedToUTC = val;
    }

    void __set_unit(const TimeUnit& val) {
        _unit = val;
    }

    bool operator==(const TimeType& rhs) const {
        if (!(_isAdjustedToUTC == rhs._isAdjustedToUTC))
            return false;
        if (!(_unit == rhs._unit))
            return false;
        return true;
    }
    bool operator!=(const TimeType& rhs) const {
        return !(*this == rhs);
    }
};

class TimestampType {
public:
    TimestampType() : _isAdjustedToUTC(0) {}
    virtual ~TimestampType() {}
    bool _isAdjustedToUTC;
    TimeUnit _unit;

    void __set_isAdjustedToUTC(const bool val) {
        _isAdjustedToUTC = val;
    }

    void __set_unit(const TimeUnit& val) {
        _unit = val;
    }

    bool operator==(const TimestampType& rhs) const {
        if (!(_isAdjustedToUTC == rhs._isAdjustedToUTC))
            return false;
        if (!(_unit == rhs._unit))
            return false;
        return true;
    }
    bool operator!=(const TimestampType& rhs) const {
        return !(*this == rhs);
    }
};

class IntType {
public:
    IntType() : _bitWidth(0), _isSigned(0) {}
    virtual ~IntType() {}
    int8_t _bitWidth;
    bool _isSigned;

    void __set_bitWidth(const int8_t val) {
        _bitWidth = val;
    }

    void __set_isSigned(const bool val) {
        _isSigned = val;
    }

    bool operator==(const IntType& rhs) const {
        if (!(_bitWidth == rhs._bitWidth))
            return false;
        if (!(_isSigned == rhs._isSigned))
            return false;
        return true;
    }
    bool operator!=(const IntType& rhs) const {
        return !(*this == rhs);
    }
};

class NullType {
public:
    NullType() {}
    virtual ~NullType() {}
    bool operator==(const NullType& /* rhs */) const {
        return true;
    }
    bool operator!=(const NullType& rhs) const {
        return !(*this == rhs);
    }
};

class JsonType {
public:
    JsonType() {}
    virtual ~JsonType() {}
    bool operator==(const JsonType& /* rhs */) const {
        return true;
    }
    bool operator!=(const JsonType& rhs) const {
        return !(*this == rhs);
    }
};

class BsonType {
public:
    BsonType() {}
    virtual ~BsonType() {}
    bool operator==(const BsonType& /* rhs */) const {
        return true;
    }
    bool operator!=(const BsonType& rhs) const {
        return !(*this == rhs);
    }
};

class UUIDType {
public:
    UUIDType() {}
    virtual ~UUIDType() {}
    bool operator==(const UUIDType& /* rhs */) const {
        return true;
    }
    bool operator!=(const UUIDType& rhs) const {
        return !(*this == rhs);
    }
};

typedef struct _LogicalType__isset {
    _LogicalType__isset()
        : STRING(false),
          MAP(false),
          LIST(false),
          ENUM(false),
          DECIMAL(false),
          DATE(false),
          TIME(false),
          TIMESTAMP(false),
          INTEGER(false),
          UNKNOWN(false),
          JSON(false),
          BSON(false),
          UUID(false) {}
    bool STRING : 1;
    bool MAP : 1;
    bool LIST : 1;
    bool ENUM : 1;
    bool DECIMAL : 1;
    bool DATE : 1;
    bool TIME : 1;
    bool TIMESTAMP : 1;
    bool INTEGER : 1;
    bool UNKNOWN : 1;
    bool JSON : 1;
    bool BSON : 1;
    bool UUID : 1;
} _LogicalType__isset;

class LogicalType {
public:
    LogicalType() {}

    virtual ~LogicalType() {}
    StringType STRING;
    MapType MAP;
    ListType LIST;
    EnumType ENUM;
    DecimalType DECIMAL;
    DateType DATE;
    TimeType TIME;
    TimestampType TIMESTAMP;
    IntType INTEGER;
    NullType UNKNOWN;
    JsonType JSON;
    BsonType BSON;
    UUIDType UUID;

    _LogicalType__isset __isset;

    bool __is_set() const {
        if (__isset.STRING || __isset.MAP || __isset.LIST || __isset.ENUM || __isset.DECIMAL || __isset.DATE || __isset.TIME || __isset.TIMESTAMP ||
            __isset.INTEGER || __isset.UNKNOWN || __isset.JSON || __isset.BSON || __isset.UUID) {
            return true;
        }
        return false;
    }

    void __set_STRING(const StringType& val) {
        STRING = val;
        __isset.STRING = true;
    }

    void __set_MAP(const MapType& val) {
        MAP = val;
        __isset.MAP = true;
    }

    void __set_LIST(const ListType& val) {
        LIST = val;
        __isset.LIST = true;
    }

    void __set_ENUM(const EnumType& val) {
        ENUM = val;
        __isset.ENUM = true;
    }

    void __set_DECIMAL(const DecimalType& val) {
        DECIMAL = val;
        __isset.DECIMAL = true;
    }

    void __set_DATE(const DateType& val) {
        DATE = val;
        __isset.DATE = true;
    }

    void __set_TIME(const TimeType& val) {
        TIME = val;
        __isset.TIME = true;
    }

    void __set_TIMESTAMP(const TimestampType& val) {
        TIMESTAMP = val;
        __isset.TIMESTAMP = true;
    }

    void __set_INTEGER(const IntType& val) {
        INTEGER = val;
        __isset.INTEGER = true;
    }

    void __set_UNKNOWN(const NullType& val) {
        UNKNOWN = val;
        __isset.UNKNOWN = true;
    }

    void __set_JSON(const JsonType& val) {
        JSON = val;
        __isset.JSON = true;
    }

    void __set_BSON(const BsonType& val) {
        BSON = val;
        __isset.BSON = true;
    }

    void __set_UUID(const UUIDType& val) {
        UUID = val;
        __isset.UUID = true;
    }

    bool operator==(const LogicalType& rhs) const {
        if (__isset.STRING != rhs.__isset.STRING)
            return false;
        else if (__isset.STRING && !(STRING == rhs.STRING))
            return false;
        if (__isset.MAP != rhs.__isset.MAP)
            return false;
        else if (__isset.MAP && !(MAP == rhs.MAP))
            return false;
        if (__isset.LIST != rhs.__isset.LIST)
            return false;
        else if (__isset.LIST && !(LIST == rhs.LIST))
            return false;
        if (__isset.ENUM != rhs.__isset.ENUM)
            return false;
        else if (__isset.ENUM && !(ENUM == rhs.ENUM))
            return false;
        if (__isset.DECIMAL != rhs.__isset.DECIMAL)
            return false;
        else if (__isset.DECIMAL && !(DECIMAL == rhs.DECIMAL))
            return false;
        if (__isset.DATE != rhs.__isset.DATE)
            return false;
        else if (__isset.DATE && !(DATE == rhs.DATE))
            return false;
        if (__isset.TIME != rhs.__isset.TIME)
            return false;
        else if (__isset.TIME && !(TIME == rhs.TIME))
            return false;
        if (__isset.TIMESTAMP != rhs.__isset.TIMESTAMP)
            return false;
        else if (__isset.TIMESTAMP && !(TIMESTAMP == rhs.TIMESTAMP))
            return false;
        if (__isset.INTEGER != rhs.__isset.INTEGER)
            return false;
        else if (__isset.INTEGER && !(INTEGER == rhs.INTEGER))
            return false;
        if (__isset.UNKNOWN != rhs.__isset.UNKNOWN)
            return false;
        else if (__isset.UNKNOWN && !(UNKNOWN == rhs.UNKNOWN))
            return false;
        if (__isset.JSON != rhs.__isset.JSON)
            return false;
        else if (__isset.JSON && !(JSON == rhs.JSON))
            return false;
        if (__isset.BSON != rhs.__isset.BSON)
            return false;
        else if (__isset.BSON && !(BSON == rhs.BSON))
            return false;
        if (__isset.UUID != rhs.__isset.UUID)
            return false;
        else if (__isset.UUID && !(UUID == rhs.UUID))
            return false;
        return true;
    }
    bool operator!=(const LogicalType& rhs) const {
        return !(*this == rhs);
    }
};

typedef struct _SchemaElement__isset {
    _SchemaElement__isset()
        : _type(false),
          _typeLength(false),
          _repetitionType(false),
          _numChildren(false),
          _convertedType(false),
          _scale(false),
          _precision(false),
          _fieldId(false),
          _logicalType(false) {}
    bool _type : 1;
    bool _typeLength : 1;
    bool _repetitionType : 1;
    bool _numChildren : 1;
    bool _convertedType : 1;
    bool _scale : 1;
    bool _precision : 1;
    bool _fieldId : 1;
    bool _logicalType : 1;
} _SchemaElement__isset;

struct PageType {
    enum type { DATA_PAGE = 0, INDEX_PAGE = 1, DICTIONARY_PAGE = 2, DATA_PAGE_V2 = 3 };
};

struct CompressionCodec {
    enum type { UNCOMPRESSED = 0, SNAPPY = 1, GZIP = 2, LZO = 3, BROTLI = 4, LZ4 = 5, ZSTD = 6, LZ4_RAW = 7 };
};

static inline Compression::type CompressionConvert(CompressionCodec::type type) {
    switch (type) {
        case CompressionCodec::UNCOMPRESSED:
            return Compression::UNCOMPRESSED;
        case CompressionCodec::SNAPPY:
            return Compression::SNAPPY;
        case CompressionCodec::GZIP:
            return Compression::GZIP;
        case CompressionCodec::LZO:
            return Compression::LZO;
        case CompressionCodec::BROTLI:
            return Compression::BROTLI;
        case CompressionCodec::LZ4_RAW:
            return Compression::LZ4;
        case CompressionCodec::LZ4:
            return Compression::LZ4_HADOOP;
        case CompressionCodec::ZSTD:
            return Compression::ZSTD;
        default:
            return Compression::UNCOMPRESSED;
    }
}

static inline CompressionCodec::type CompressionConvert(Compression::type type) {
    switch (type) {
        case Compression::UNCOMPRESSED:
            return CompressionCodec::UNCOMPRESSED;
        case Compression::SNAPPY:
            return CompressionCodec::SNAPPY;
        case Compression::GZIP:
            return CompressionCodec::GZIP;
        case Compression::LZO:
            return CompressionCodec::LZO;
        case Compression::BROTLI:
            return CompressionCodec::BROTLI;
        case Compression::LZ4:
            return CompressionCodec::LZ4_RAW;
        case Compression::LZ4_HADOOP:
            return CompressionCodec::LZ4;
        case Compression::ZSTD:
            return CompressionCodec::ZSTD;
        default:
            return CompressionCodec::UNCOMPRESSED;
    }
}

typedef struct _KeyValue__isset {
    _KeyValue__isset() : _value(false) {}
    bool _value : 1;
} _KeyValue__isset;

typedef struct _ColumnMetaData__isset {
    _ColumnMetaData__isset()
        : _keyValueMetadata(false),
          _indexPageOffset(false),
          _dictionaryPageOffset(false),
          _statistics(false),
          _encodingStats(false),
          _bloomFilterOffset(false) {}
    bool _keyValueMetadata : 1;
    bool _indexPageOffset : 1;
    bool _dictionaryPageOffset : 1;
    bool _statistics : 1;
    bool _encodingStats : 1;
    bool _bloomFilterOffset : 1;
} _ColumnMetaData__isset;

typedef struct _EncryptionWithColumnKey__isset {
    _EncryptionWithColumnKey__isset() : _keyMetadata(false) {}
    bool _keyMetadata : 1;
} _EncryptionWithColumnKey__isset;

typedef struct _ColumnCryptoMetaData__isset {
    _ColumnCryptoMetaData__isset() : ENCRYPTION_WITH_FOOTER_KEY(false), ENCRYPTION_WITH_COLUMN_KEY(false) {}
    bool ENCRYPTION_WITH_FOOTER_KEY : 1;
    bool ENCRYPTION_WITH_COLUMN_KEY : 1;
} _ColumnCryptoMetaData__isset;

typedef struct _ColumnChunk__isset {
    _ColumnChunk__isset()
        : _filePath(false),
          _metaData(false),
          _offsetIndexOffset(false),
          _offsetIndexLength(false),
          _columnIndexOffset(false),
          _columnIndexLength(false),
          _cryptoMetadata(false),
          _encryptedColumnMetadata(false) {}
    bool _filePath : 1;
    bool _metaData : 1;
    bool _offsetIndexOffset : 1;
    bool _offsetIndexLength : 1;
    bool _columnIndexOffset : 1;
    bool _columnIndexLength : 1;
    bool _cryptoMetadata : 1;
    bool _encryptedColumnMetadata : 1;
} _ColumnChunk__isset;

typedef struct _Statistics__isset {
    _Statistics__isset() : _max(false), _min(false), _nullCount(false), _distinctCount(false), _maxValue(false), _minValue(false) {}
    bool _max : 1;
    bool _min : 1;
    bool _nullCount : 1;
    bool _distinctCount : 1;
    bool _maxValue : 1;
    bool _minValue : 1;
} _Statistics__isset;

typedef struct _DictionaryPageHeader__isset {
    _DictionaryPageHeader__isset() : _isSorted(false) {}
    bool _isSorted : 1;
} _DictionaryPageHeader__isset;

typedef struct _DataPageHeader__isset {
    _DataPageHeader__isset() : _statistics(false) {}
    bool _statistics : 1;
} _DataPageHeader__isset;

typedef struct _DataPageHeaderV2__isset {
    _DataPageHeaderV2__isset() : _isCompressed(true), _statistics(false) {}
    bool _isCompressed : 1;
    bool _statistics : 1;
} _DataPageHeaderV2__isset;

typedef struct _PageHeader__isset {
    _PageHeader__isset() : _crc(false), _dataPageHeader(false), _indexPageHeader(false), _dictionaryPageHeader(false), _dataPageHeaderV2(false) {}
    bool _crc : 1;
    bool _dataPageHeader : 1;
    bool _indexPageHeader : 1;
    bool _dictionaryPageHeader : 1;
    bool _dataPageHeaderV2 : 1;
} _PageHeader__isset;

class PageHeader;

typedef struct _RowGroup__isset {
    _RowGroup__isset() : _sortingColumns(false), _fileOffset(false), _totalCompressedSize(false), _ordinal(false) {}
    bool _sortingColumns : 1;
    bool _fileOffset : 1;
    bool _totalCompressedSize : 1;
    bool _ordinal : 1;
} _RowGroup__isset;

typedef struct _ColumnOrder__isset {
    _ColumnOrder__isset() : TYPE_ORDER(false) {}
    bool TYPE_ORDER : 1;
} _ColumnOrder__isset;

typedef struct _FileMetaData__isset {
    _FileMetaData__isset() : _keyValueMetadata(false), _createdBy(false), _columnOrders(false), _encryptionAlgorithm(false), _footerSigningKeyMetadata(false) {}
    bool _keyValueMetadata : 1;
    bool _createdBy : 1;
    bool _columnOrders : 1;
    bool _encryptionAlgorithm : 1;
    bool _footerSigningKeyMetadata : 1;
} _FileMetaData__isset;

// std::ostream& operator<<(std::ostream& out, const PageHeader& obj);

}  // end namespace clearParquet
