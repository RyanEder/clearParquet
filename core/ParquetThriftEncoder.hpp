#pragma once

#include <iostream>
#include <string>
#include <vector>

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

class ParquetThriftEncoder {
public:
    void clearBuffer() {
        _buffer.clear();
        _fieldIds.clear();
    }

    // Serialize a boolean into a byte stream using Compact Protocol.
    void writeBool(bool value) {
        writeFieldBegin("", (value == 0) ? ThriftFieldType::T_BOOL_FALSE : ThriftFieldType::T_BOOL, _boolFieldId);
    }

    void writeByteDirect(uint8_t value) {
        _buffer.push_back(static_cast<uint8_t>(value));
    }
    // Serialize a byte (uint8_t) into a byte stream using Compact Protocol.
    void writeByte(uint8_t value) {
        writeByteDirect(value);
        //_buffer.push_back(static_cast<uint8_t>((static_cast<uint32_t>(ThriftFieldType::T_BYTE) << 4) | (value & 0x0F)));
    }

    // Serialize an int16_t into a byte stream using Compact Protocol.
    void writeI16(int16_t value) {
        writeVarint(zigzagEncode(value));
    }

    // Serialize an int32_t into a byte stream using Compact Protocol.
    void writeI32(int32_t value) {
        writeVarint(zigzagEncode(value));
    }

    // Serialize an int64_t into a byte stream using Compact Protocol.
    void writeI64(int64_t value) {
        writeVarint(zigzagEncode(value));
    }

    void writeStructBegin(const std::string& fieldName) {
        _fieldIds.push_back(_previousFieldId);
        _previousFieldId = 0;
    }

    void writeStructEnd() {
        _previousFieldId = _fieldIds.back();
        _fieldIds.pop_back();
        // pop from stack.
    }
    void writeListEnd() {}
    void writeListBegin(ThriftFieldType fieldType, uint32_t count) {
        if (count <= 14) {
            writeByteDirect(count << 4 | static_cast<uint32_t>(fieldType));
            // writeByteDirect(static_cast<uint8_t>count
        } else {
            writeByte(0xf0 | static_cast<uint32_t>(fieldType));
            writeVarint(count);
        }
    }
    void writeFieldStop() {
        writeByteDirect(static_cast<uint8_t>(ThriftFieldType::T_STOP));
    }  // write stop bit
    void writeBinary(const std::string& data) {}

    void writeFieldBegin(const std::string& name, ThriftFieldType fieldType, int16_t fieldId) {
        if (fieldType == ThriftFieldType::T_BOOL) {
            _boolFieldId = fieldId;
            // wait for the value itself, bools are strange as the true/false have their own types.
            return;
        }
        int16_t fieldDelta = fieldId - _previousFieldId;
        int8_t fieldTypeByte = static_cast<int8_t>(fieldType);

        if (fieldDelta >= -15 && fieldDelta <= 15) {
            writeByteDirect(static_cast<uint8_t>((static_cast<uint16_t>(fieldDelta) << 4) | fieldTypeByte));
        } else {
            writeByte(static_cast<uint8_t>(fieldTypeByte));
            writeVarint(static_cast<int32_t>(fieldDelta));
        }

        _previousFieldId = fieldId;
    }

    // Serialize a string into a byte stream using Compact Protocol.
    void writeString(const std::string& value) {
        writeVarint(static_cast<int32_t>(value.length()));
        for (char c : value) {
            _buffer.push_back(static_cast<uint8_t>(c));
        }
    }
    uint8_t* getBufferData() {
        return _buffer.data();
    }
    size_t getBufferSize() {
        return _buffer.size();
    }

private:
    // Helper function to write a varint (variable-length integer) to the buffer.
    void writeVarint(int32_t value) {
        while (true) {
            if ((value & ~0x7F) == 0) {
                _buffer.push_back(static_cast<uint8_t>(value));
                return;
            }
            _buffer.push_back(static_cast<uint8_t>((value & 0x7F) | 0x80));
            value >>= 7;
        }
    }

    // Helper function for zigzag encoding for signed integers.
    int32_t zigzagEncode(int32_t value) {
        return (value << 1) ^ (value >> 31);
    }

    int64_t zigzagEncode(int64_t value) {
        return (value << 1) ^ (value >> 63);
    }

    int16_t _previousFieldId = 0;
    std::vector<uint8_t> _buffer;
    std::vector<int16_t> _fieldIds;
    uint16_t _boolFieldId = 0;
};
