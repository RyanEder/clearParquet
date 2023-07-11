#pragma once
#include "ParquetFileWriter.hpp"

#include <iostream>
#include <string>
#include <string_view>

namespace clearParquet {

class StreamWriter {
   public:
    StreamWriter() = default;
    explicit StreamWriter(std::unique_ptr<ParquetFileWriter> writer) : _fileWriter(std::move(writer)) {}

    ~StreamWriter() = default;

    StreamWriter(StreamWriter&&) = default;
    StreamWriter& operator=(StreamWriter&&) = default;

    StreamWriter& operator<<(bool v) {
        _fileWriter->_boolCols->Store(v, sizeof(bool));
        return *this;
    }

    StreamWriter& operator<<(int8_t v) {
        _fileWriter->_int32Cols->Store((int32_t)v, sizeof(int32_t));
        return *this;
    }

    StreamWriter& operator<<(uint8_t v) {
        _fileWriter->_int32Cols->Store((int32_t)v, sizeof(int32_t));
        return *this;
    }

    StreamWriter& operator<<(int16_t v) {
        _fileWriter->_int32Cols->Store((int32_t)v, sizeof(int32_t));
        return *this;
    }

    StreamWriter& operator<<(uint16_t v) {
        _fileWriter->_int32Cols->Store((int32_t)v, sizeof(int32_t));
        return *this;
    }

    StreamWriter& operator<<(int32_t v) {
        _fileWriter->_int32Cols->Store(v, sizeof(int32_t));
        return *this;
    }

    StreamWriter& operator<<(uint32_t v) {
        _fileWriter->_int32Cols->Store((int32_t)v, sizeof(int32_t));
        return *this;
    }

    StreamWriter& operator<<(int64_t v) {
        _fileWriter->_int64Cols->Store(v, sizeof(int64_t));
        return *this;
    }

    StreamWriter& operator<<(uint64_t v) {
        _fileWriter->_int64Cols->Store((int64_t)v, sizeof(int64_t));
        return *this;
    }

    StreamWriter& operator<<(float v) {
        _fileWriter->_floatCols->Store(v, sizeof(float));
        return *this;
    }

    StreamWriter& operator<<(double v) {
        _fileWriter->_doubleCols->Store(v, sizeof(double));
        return *this;
    }

    StreamWriter& operator<<(const char* v) {
        stringData = v;
        _fileWriter->_strCols->Store(stringData, stringData.length() + 4);
        return *this;
    }
    StreamWriter& operator<<(const std::string& v) {
        _fileWriter->_strCols->Store(v, v.length() + 4);
        return *this;
    }
    StreamWriter& operator<<(::std::string_view v) {
        stringData = v;
        _fileWriter->_strCols->Store(stringData, stringData.length() + 4);
        return *this;
    }
    template <class T>
    void SerializeAndWrite(const T* obj) {
        _fileWriter->SerializeAndWrite(obj);
    }
    void WriteBuffer(uint32_t len, const uint8_t* buffer) {
        _fileWriter->WriteBuffer(len, buffer);
    }
    void WriteString() {
        uint32_t len = stringData.length();
        WriteBuffer(4, (const uint8_t*)&len);
        WriteBuffer(len, (const uint8_t*)stringData.c_str());
    }

    void WriteInt() {
        WriteBuffer(8, (uint8_t*)&val);
    }

    void EndRow() {
        _fileWriter->EndRow();
        if (_fileWriter->GetSize() >= _maxRowGroupSize) {
            EndRowGroup();
        }
    }

    void EndRowGroup() {
        _fileWriter->EndRowGroup();
    }

    void SetMaxRowGroupSize(uint64_t rowsize) {
        _maxRowGroupSize = rowsize;
    }

   private:
    std::unique_ptr<ParquetFileWriter> _fileWriter;
    bool _switch = 0;
    uint64_t _maxRowGroupSize = 0;
    std::string stringData;
    uint64_t val;
};

struct EndRowType {};
constexpr EndRowType EndRow = {};
struct EndRowGroupType {};
constexpr EndRowGroupType EndRowGroup = {};

StreamWriter& operator<<(StreamWriter& os, EndRowType) {
    os.EndRow();
    return os;
}

StreamWriter& operator<<(StreamWriter& os, EndRowGroupType) {
    os.EndRow();
    return os;
}

}  // end namespace clearParquet
