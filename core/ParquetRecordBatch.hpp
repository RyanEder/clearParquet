#pragma once
#include "ParquetDataStore.hpp"
#include "ParquetSchema.hpp"

namespace clearParquet {

class RecordBatch {
public:
    RecordBatch(std::vector<SchemaElement>& schemas, size_t reserveSize = 1024LL * 1024LL)
        : _boolCols(nullptr), _floatCols(nullptr), _doubleCols(nullptr), _int32Cols(nullptr), _int64Cols(nullptr), _strCols(nullptr) {
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
                        _boolCols = std::make_shared<DataStore<bool>>(reserveSize, counts[utype]);
                        break;
                    case Type::INT32:
                        _int32Cols = std::make_shared<DataStore<int32_t>>(reserveSize, counts[utype]);
                        break;
                    case Type::INT64:
                        _int64Cols = std::make_shared<DataStore<int64_t>>(reserveSize, counts[utype]);
                        break;
                    case Type::INT96:
                        throw std::invalid_argument("Unsupported type: INT96");
                        break;
                    case Type::FLOAT:
                        _floatCols = std::make_shared<DataStore<float>>(reserveSize, counts[utype]);
                        break;
                    case Type::DOUBLE:
                        _doubleCols = std::make_shared<DataStore<double>>(reserveSize, counts[utype]);
                        break;
                    case Type::BYTE_ARRAY:
                        _strCols = std::make_shared<DataStore<std::string>>(reserveSize, counts[utype]);
                        break;
                    case Type::FIXED_LEN_BYTE_ARRAY:
                        throw std::invalid_argument("Unsupported type: FIXED_LEN_BYTE_ARRAY");
                        break;
                    default:
                        break;
                }
            }
        }
        _allCols = {_boolCols, _int32Cols, _int64Cols, nullptr, _floatCols, _doubleCols, _strCols, nullptr, nullptr};
    }

    template <typename T, typename F, std::size_t... I>
    constexpr void visit_impl(T& tup, const size_t idx, F fun, std::index_sequence<I...>) {
        ((I == idx ? fun(std::get<I>(tup)) : void()), ...);
    }

    template <typename F, typename... Ts, typename Indices = std::make_index_sequence<sizeof...(Ts)>>
    constexpr void visit_at(std::tuple<Ts...>& tup, const size_t idx, F fun) {
        visit_impl(tup, idx, fun, Indices{});
    }

    template <typename F, typename... Ts, typename Indices = std::make_index_sequence<sizeof...(Ts)>>
    constexpr void visit_at(const std::tuple<Ts...>& tup, const size_t idx, F fun) {
        visit_impl(tup, idx, fun, Indices{});
    }

    void ParseBuffer(Type::type type, char* buffer, size_t numValues) {
        if (type == Type::BYTE_ARRAY) {
            size_t offset = 0;
            for (size_t i = 0; i < numValues; ++i) {
                size_t len = *(uint32_t*)(buffer + offset);
                offset += 4;
                std::string str(buffer + offset, len);
                offset += len;
                _strCols->StoreSimple(str, len);
            }
            _strCols->IncrementCol();
        } else if (type == Type::BOOLEAN) {
            _boolCols->StoreBoolBlock(buffer, numValues);
        } else {
            visit_at(_allCols, (size_t)type, [this, &numValues, &buffer](auto&& arg) { arg->StoreBlock(buffer, numValues); });
        }
    }

public:
    std::shared_ptr<DataStore<bool>> _boolCols;
    std::shared_ptr<DataStore<float>> _floatCols;
    std::shared_ptr<DataStore<double>> _doubleCols;
    std::shared_ptr<DataStore<int32_t>> _int32Cols;
    std::shared_ptr<DataStore<int64_t>> _int64Cols;
    std::shared_ptr<DataStore<std::string>> _strCols;
    std::tuple<std::shared_ptr<DataStore<bool>>, std::shared_ptr<DataStore<int32_t>>, std::shared_ptr<DataStore<int64_t>>, std::shared_ptr<DataStore<char>>,
               std::shared_ptr<DataStore<float>>, std::shared_ptr<DataStore<double>>, std::shared_ptr<DataStore<std::string>>, std::shared_ptr<DataStore<char>>,
               std::shared_ptr<DataStore<char>>>
        _allCols;
};

}  // end namespace clearParquet
