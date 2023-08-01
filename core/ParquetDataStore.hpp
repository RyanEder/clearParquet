#pragma once
#include <vector>
#include "ParquetTypes.hpp"

namespace clearParquet {

// Simple templated data storage
template <class T>
class DataStore {
public:
    DataStore(size_t reserveSize, uint32_t columnCount) : _columnCount(columnCount), _col(0), _sizeIndexer(0) {
        if (columnCount > 0) {
            _store.resize(columnCount);
            _sizes.resize(columnCount);
            for (auto& size : _sizes) {
                size = 0;
            }
        }
        if (reserveSize > 0) {
            for (auto& colStore : _store) {
                colStore.reserve(reserveSize);
            }
        }
    }

    void Store(const T& val, uint32_t size) {
        _store[_col].emplace_back(val);
        _sizes[_col] += size;
        IncrementCol();
    }
    void Store(T& val, uint32_t size) {
        _store[_col].emplace_back(val);
        _sizes[_col] += size;
        IncrementCol();
    }
    void StoreBlock(char* block, size_t numValues) {
        auto size = sizeof(T);
        size_t offset = 0;
        for (size_t i = 0; i < numValues; ++i) {
            _store[_col].emplace_back(*(T*)(block + offset));
            offset += size;
        }
        IncrementCol();
    }
    // Should only be called on bool specifically.
    void StoreBoolBlock(char* block, size_t numValues) {
        for (size_t i = 0; i < numValues; ++i) {
            size_t byteIndex = i / 8;
            size_t bitPos = i % 8;
            char byteValue = block[byteIndex];
            _store[_col].emplace_back((T)((byteValue & (1 << bitPos)) != 0));
        }
        IncrementCol();
    }
    void IncrementCol() {
        if (++_col % _columnCount == 0) {
            _col = 0;
        }
    }

    void StoreSimple(const T& val, uint32_t size) {
        _store[_col].emplace_back(val);
        _sizes[_col] += size;
    }

    std::vector<T>& Get() {
        // Get impliles a full and in-order traversal of the data structure.
        // Wrapping after a full read.
        auto& vec = _store[_col];
        IncrementCol();
        return vec;
    }

    uint64_t GetSize() {
        // sizeIndexer implies a full and in-order traversal of the data structure.
        // Wrapping after getting all sizes.
        uint64_t size = _sizes[_sizeIndexer];
        if (++_sizeIndexer % _columnCount == 0) {
            _sizeIndexer = 0;
        }
        return size;
    }

    uint8_t GetSizeOf() {
        return sizeof(T);
    }

    void Clear() {
        for (auto& col : _store) {
            col.clear();
        }
        for (auto& size : _sizes) {
            size = 0;
        }
    }
    size_t ValueCount() {
        return _store.size();
    }

public:
    std::vector<std::vector<T> > _store;
    std::vector<uint64_t> _sizes;
    uint32_t _columnCount;
    uint32_t _col;
    uint32_t _sizeIndexer;
};

}  // end namespace clearParquet
