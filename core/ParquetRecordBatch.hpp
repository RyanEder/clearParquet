#pragma once

#include <iomanip>
#include <variant>

#if defined(PARQUET_SNAPPY_COMPRESSION)
#include <snappy.h>
#endif
#if defined(PARQUET_ZSTD_COMPRESSION)
#include <zstd.h>
#endif

#include "ParquetDataStore.hpp"
#include "ParquetSchema.hpp"
#include "ParquetArray.hpp"

namespace clearParquet {

class RecordBatch {
public:
    RecordBatch(std::vector<SchemaElement>& schemas, Compression::type codec)
        : _boolIndexer(0),
          _floatIndexer(0),
          _doubleIndexer(0),
          _strIndexer(0),
          _int32Indexer(0),
          _int64Indexer(0),
          _maxValues(0),
          _maxWidth(10),
#if defined(PARQUET_ZSTD_COMPRESSION)
          _dctx(nullptr),
#endif
          _codec(codec) {
        // Count up each type of column
        uint32_t counts[(uint8_t)Type::NONE] = {0};
        for (uint32_t i = 0; i < schemas.size() - 1; ++i) {
            const auto& element = schemas[i + 1];
            counts[(uint8_t)element._type]++;
        }

        // Build out initial storage.
        for (uint8_t utype = 0; utype < (uint8_t)Type::NONE; ++utype) {
            if (counts[utype] > 0) {
                Type::type ctype = (Type::type)utype;
                switch (ctype) {
                    case Type::BOOLEAN:
                        for (uint32_t i = 0; i < counts[utype]; ++i) {
                            _boolCols.push_back(new BoolArray);
                        }
                        break;
                    case Type::INT32:
                        for (uint32_t i = 0; i < counts[utype]; ++i) {
                            _int32Cols.push_back(new Int32Array);
                        }
                        break;
                    case Type::INT64:
                        for (uint32_t i = 0; i < counts[utype]; ++i) {
                            _int64Cols.push_back(new Int64Array);
                        }
                        break;
                    case Type::INT96:
                        throw std::invalid_argument("Unsupported type: INT96");
                        break;
                    case Type::FLOAT:
                        for (uint32_t i = 0; i < counts[utype]; ++i) {
                            _floatCols.push_back(new FloatArray);
                        }
                        break;
                    case Type::DOUBLE:
                        for (uint32_t i = 0; i < counts[utype]; ++i) {
                            _doubleCols.push_back(new DoubleArray);
                        }
                        break;
                    case Type::BYTE_ARRAY:
                        for (uint32_t i = 0; i < counts[utype]; ++i) {
                            _strCols.push_back(new StrArray);
                        }
                        break;
                    case Type::FIXED_LEN_BYTE_ARRAY:
                        throw std::invalid_argument("Unsupported type: FIXED_LEN_BYTE_ARRAY");
                        break;
                    default:
                       break;
                }
            }
        }
#if defined(PARQUET_ZSTD_COMPRESSION)
        _dctx = ZSTD_createDCtx();
        if (_dctx == nullptr) {
            throw std::runtime_error("Could not create the ZSTD context.");
        }
#endif
    }
    void IncrementIndexer(size_t& indexer, std::vector<Array*>& arr) {
        if (++indexer == arr.size()) {
            indexer = 0;
        }
    }

    void ParseBuffer(Type::type type, char* cBuffer, size_t numValues, std::string name, size_t dataSize) {
        char* buffer = cBuffer;
        if (_codec == Compression::ZSTD) {
#if defined(PARQUET_ZSTD_COMPRESSION)
            uint64_t rSize = ZSTD_getFrameContentSize(cBuffer, dataSize);

            buffer = (char*)malloc(rSize);
            ZSTD_decompressDCtx(_dctx, buffer, rSize, cBuffer, dataSize);
#else
            throw std::runtime_error("File compressed in a form this reader cannot uncompress.");
#endif
        }
        else if (_codec == Compression::SNAPPY) {
#if defined(PARQUET_SNAPPY_COMPRESSION)
            size_t rSize = 0;
            snappy::GetUncompressedLength(cBuffer, dataSize, &rSize);
            buffer = (char*)malloc(rSize);
            snappy::RawUncompress(cBuffer, dataSize, buffer);
#else
            throw std::runtime_error("File compressed in a form this reader cannot uncompress.");
#endif
        }
        else if (_codec != Compression::UNCOMPRESSED) {
            throw std::runtime_error("File compressed in a form this reader cannot uncompress.");
            return;
        }
        if (numValues > _maxValues) {
            _maxValues = numValues;
        }
        if (type == Type::BYTE_ARRAY) {
            auto &col = *static_cast<StrArray*>(_strCols[_strIndexer]);
            size_t offset = 0;
            for (size_t i = 0; i < numValues; ++i) {
                size_t len = *(uint32_t*)(buffer + offset);
                offset += 4;
                std::string str(buffer + offset, len);
                offset += len;
                col.Store(str);
                if (len > _maxWidth) {
                    _maxWidth = len;
                }
            }
            col.SetType(Type::BYTE_ARRAY);
            _orderedCols.push_back(&col);
            IncrementIndexer(_strIndexer, _strCols);
        } else if (Type::BOOLEAN) {
            auto& col = *static_cast<BoolArray*>(_boolCols[_boolIndexer]);
            col.StoreBoolBlock(buffer, numValues);
            col.SetType(Type::BOOLEAN);
            _orderedCols.push_back(&col);
            IncrementIndexer(_boolIndexer, _boolCols);
        } else if (type == Type::INT32) {
            auto& col = *static_cast<Int32Array*>(_int32Cols[_int32Indexer]);
            col.StoreBlock(buffer, numValues);
            col.SetType(Type::INT32);
            _orderedCols.push_back(&col);
            IncrementIndexer(_int32Indexer, _int32Cols);
        } else if (type == Type::INT64) {
            auto& col = *static_cast<Int64Array*>(_int64Cols[_int64Indexer]);
            col.StoreBlock(buffer, numValues);
            col.SetType(Type::INT64);
            _orderedCols.push_back(&col);
            IncrementIndexer(_int64Indexer, _int64Cols);
        } else if (type == Type::DOUBLE) {
            auto& col = *static_cast<DoubleArray*>(_doubleCols[_doubleIndexer]);
            col.StoreBlock(buffer, numValues);
            col.SetType(Type::DOUBLE);
            _orderedCols.push_back(&col);
            IncrementIndexer(_doubleIndexer, _doubleCols);
        } else if (type == Type::FLOAT) {
            auto& col = *static_cast<FloatArray*>(_floatCols[_floatIndexer]);
            col.StoreBlock(buffer, numValues);
            col.SetType(Type::FLOAT);
            _orderedCols.push_back(&col);
            IncrementIndexer(_floatIndexer, _floatCols);
        } else if (type == Type::INT96 || type ==  Type::FIXED_LEN_BYTE_ARRAY) {
            throw std::invalid_argument("Unsupported type");
        }
        _orderedCols.back()->SetName(name);
    }

    std::vector<Array*>& Columns() {
        return _orderedCols;
    }

    Array* Column(uint64_t i) {
        if (i > _orderedCols.size()) {
            throw std::runtime_error("Column index out of bounds.");
        }
        return _orderedCols[i];
    }

    std::string& ColumnName(uint64_t i) {
        if (i > _orderedCols.size()) {
            throw std::runtime_error("Column index out of bounds.");
        }
        return _orderedCols[i]->ToString();
    }

    uint64_t NumColumns() { 
        return _orderedCols.size();
    }
    
    void PrintBatch() {
        std::cout << "| ";
        for (const auto& arr : _orderedCols) {
            std::cout << std::left << std::setw(_maxWidth) << arr->ToString() << " | ";
        }
        std::cout << std::endl;
        for (size_t i = 0; i < _maxValues; ++i) {
            std::cout << "| ";
            for (size_t j = 0; j < _orderedCols.size(); ++j) {
                const Array* base = _orderedCols[j];
                if (base == nullptr) { continue; }
                const auto& type = base->GetType();
                if (type == Type::BYTE_ARRAY) {
                    const auto& arr = *static_cast<const StrArray*>(base);
                    std::cout << std::setw(_maxWidth) << arr.Value(i) << " | ";
                } else if (type == Type::BOOLEAN) {
                    const auto& arr = *static_cast<const BoolArray*>(base);
                    std::cout << std::setw(_maxWidth) << arr.Value(i) << " | ";
                } else if (type == Type::INT32) {
                    const auto& arr = *static_cast<const Int32Array*>(base);
                    std::cout << std::setw(_maxWidth) << arr.Value(i) << " | ";
                } else if (type == Type::INT64) {
                    const auto& arr = *static_cast<const Int64Array*>(base);
                    std::cout << std::setw(_maxWidth) << arr.Value(i) << " | ";
                } else if (type == Type::DOUBLE) {
                    const auto& arr = *static_cast<const DoubleArray*>(base);
                    std::cout << std::setw(_maxWidth) << arr.Value(i) << " | ";
                } else if (type == Type::FLOAT) {
                    const auto& arr = *static_cast<const FloatArray*>(base);
                    std::cout << std::setw(_maxWidth) << arr.Value(i) << " | ";
                } else if (type == Type::INT96 || type == Type::FIXED_LEN_BYTE_ARRAY) {
                    throw std::invalid_argument("Unsupported type");
                }
            }
            std::cout << std::endl;
        }
    }

public:
    std::vector<Array*> _boolCols;
    std::vector<Array*> _floatCols;
    std::vector<Array*> _doubleCols;
    std::vector<Array*> _strCols;
    std::vector<Array*> _int32Cols;
    std::vector<Array*> _int64Cols;
    size_t _boolIndexer;
    size_t _floatIndexer;
    size_t _doubleIndexer;
    size_t _strIndexer;
    size_t _int32Indexer;
    size_t _int64Indexer;

    std::vector<Array*> _orderedCols;
    size_t _maxValues;
    size_t _maxWidth;
#if defined(PARQUET_ZSTD_COMPRESSION)
    ZSTD_DCtx* _dctx;
#endif
    Compression::type _codec;
};

}  // end namespace clearParquet
