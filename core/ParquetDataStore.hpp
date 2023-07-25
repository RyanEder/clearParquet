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
        if (++_col % _columnCount == 0) {
            _col = 0;
        }
    }
    void Store(T& val, uint32_t size) {
        _store[_col].emplace_back(val);
        _sizes[_col] += size;
        if (++_col % _columnCount == 0) {
            _col = 0;
        }
    }
    std::vector<T>& Get() {
        // Get impliles a full and in-order traversal of the data structure.
        // Wrapping after a full read.
        auto& vec = _store[_col];
        if (++_col % _columnCount == 0) {
            _col = 0;
        }
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

public:
    std::vector<std::vector<T> > _store;
    std::vector<uint64_t> _sizes;
    uint32_t _columnCount;
    uint32_t _col;
    uint32_t _sizeIndexer;
};

}  // end namespace clearParquet
