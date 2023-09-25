#pragma once

#include <iostream>
#include <string>
#include <vector>
#include "ParquetTypes.hpp"

namespace clearParquet {

class ParquetThriftDecoder {
public:
    void clearBuffer() {
        _buffer = nullptr;
        _offset = 0;
    }

    void setBuffer(char* buffer) {
        _buffer = buffer;
        _offset = 0;
    }
    bool decodeBool() {
        return decodeBool(_buffer, _offset);
    }
    uint8_t decodeByte() {
        return decodeByte(_buffer, _offset);
    }
    int16_t decodeI16() {
        return decodeI16(_buffer, _offset);
    }
    int32_t decodeI32() {
        return decodeI32(_buffer, _offset);
    }
    int64_t decodeI64() {
        return decodeI64(_buffer, _offset);
    }
    std::string decodeString() {
        return decodeString(_buffer, _offset);
    }
    void decodeFieldBegin() {
        int16_t a;
        ThriftFieldType b;
        decodeFieldBegin(_buffer, _offset, a, b);
    }
    void decodeListBegin(int32_t& size) {
        ThriftFieldType b;
        decodeListBegin(_buffer, _offset, b, size);
    }
    void decodeFieldStop() {
        decodeFieldStop(_buffer, _offset);
    }

    // Deserialize a boolean from the binary data.
    bool decodeBool(const char* data, size_t& offset) {
        return decodeByte(data, offset) != 0;
    }

    // Deserialize a byte (uint8_t) from the binary data.
    uint8_t decodeByte(const char* data, size_t& offset) {
        return data[offset++];
    }

    // Deserialize an int16_t from the binary data.
    int16_t decodeI16(const char* data, size_t& offset) {
        return static_cast<int16_t>(decodeVarint(data, offset));
    }

    // Deserialize an int32_t from the binary data.
    int32_t decodeI32(const char* data, size_t& offset) {
        return static_cast<int32_t>(decodeVarint(data, offset));
    }

    // Deserialize an int64_t from the binary data.
    int64_t decodeI64(const char* data, size_t& offset) {
        return static_cast<int64_t>(decodeVarint64(data, offset));
    }

    // Deserialize a string from the binary data.
    std::string decodeString(const char* data, size_t& offset) {
        int32_t length = decodeVarintRaw(data, offset);
        std::string result(data + offset, length);
        offset += length;
        return result;
    }

    // Deserialize the field ID and field type from the binary data.
    void decodeFieldBegin(const char* data, size_t& offset, int16_t& fieldId, ThriftFieldType& fieldType) {
        uint8_t byte = decodeByte(data, offset);

        fieldId = static_cast<int16_t>((byte >> 4) & 0x0F);
        uint8_t typeValue = byte & 0x0F;
        fieldType = static_cast<ThriftFieldType>(typeValue);
    }

    void decodeListBegin(const char* data, size_t& offset, ThriftFieldType& elementType, int32_t& size) {
        uint8_t byte = decodeByte(data, offset);
        size = (byte >> 4) & 0x0F;
        if (size > 14) {
            size = decodeVarint(data, offset);
        }

        elementType = static_cast<ThriftFieldType>(byte & 0x0F);
    }

    uint8_t decodeFieldStop(const char* data, size_t& offset) {
        return decodeByte(data, offset);
    }  // decode stop bit

private:
    // Helper function to read a varint (variable-length integer) from the binary data.
    int32_t decodeVarint(const char* data, size_t& offset) {
        int32_t result = 0;
        int32_t shift = 0;
        while (true) {
            uint8_t byte = static_cast<uint8_t>(data[offset++]);
            result |= static_cast<int32_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                // Zigzag decoding: Convert unsigned int to signed int
                return (result >> 1) ^ -(result & 1);
            }
            shift += 7;
        }
    }
    int64_t decodeVarint64(const char* data, size_t& offset) {
        int64_t result = 0;
        int64_t shift = 0;
        while (true) {
            uint8_t byte = static_cast<uint8_t>(data[offset++]);
            result |= static_cast<int64_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                // Zigzag decoding: Convert unsigned int to signed int
                return (result >> 1) ^ -(result & 1);
            }
            shift += 7;
        }
    }

    int32_t decodeVarintRaw(const char* data, size_t& offset) {
        int32_t result = 0;
        int32_t shift = 0;
        while (true) {
            uint8_t byte = static_cast<uint8_t>(data[offset++]);
            result |= static_cast<int32_t>(byte & 0x7F) << shift;
            if ((byte & 0x80) == 0) {
                break;
            }
            shift += 7;
        }
        return result;
    }
    char* _buffer = nullptr;
    size_t _offset = 0;
};

}  // end namespace clearParquet
