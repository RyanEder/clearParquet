#pragma once

#include <vector>
#include <any>

namespace clearParquet {

class Array {
public:
    Array() {}
    void SetType(Type::type type) {
        _type = type;
    }

    const Type::type& GetType() const { return _type; }

    void SetName(std::string& name) {
        _name = name;
    }

    std::string& ToString() {
        return _name;
    }

    virtual bool ToVector(std::shared_ptr<std::vector<bool>>& /*input*/) { return false; }
    virtual bool ToVector(std::shared_ptr<std::vector<uint32_t>>& /*input*/) { return false; }
    virtual bool ToVector(std::shared_ptr<std::vector<uint64_t>>& /*input*/) { return false; }
    virtual bool ToVector(std::shared_ptr<std::vector<float>>& /*input*/) { return false; }
    virtual bool ToVector(std::shared_ptr<std::vector<double>>& /*input*/) { return false; }
    virtual bool ToVector(std::shared_ptr<std::vector<std::string>>& /*input*/) { return false; }

    virtual uint64_t Size() const = 0;

    virtual uint32_t Int32Value(uint64_t /*i*/) const { return 0; }
    virtual uint64_t Int64Value(uint64_t /*i*/) const { return 0; }
    virtual std::string StrValue(uint64_t /*i*/) const { return {}; }
    virtual bool BoolValue(uint64_t /*i*/) const { return false; }
    virtual float FloatValue(uint64_t /*i*/) const { return 0.0; }
    virtual double DoubleValue(uint64_t /*i*/) const { return 0.0; }

    std::any Value(uint64_t i) const {
        switch (_type) {
            case Type::BOOLEAN:
                return BoolValue(i);
            case Type::INT32:
                return Int32Value(i);
            case Type::INT64:
                return Int64Value(i);
            case Type::FLOAT:
                return FloatValue(i);
            case Type::DOUBLE:
                return DoubleValue(i);
            case Type::BYTE_ARRAY:
                return StrValue(i);
            default:
                break;
        }
        return {};
    }

private:
    Type::type _type;
    std::string _name;
};

class BoolArray : public Array {
public:
    bool Value(uint64_t i) const {
        if (i > _values.size()) {
            throw std::runtime_error("Array index out of bounds.");
        }
        return _values[i];
    }

    void StoreBoolBlock(char* block, size_t numValues) {
        for (size_t i = 0; i < numValues; ++i) {
            size_t byteIndex = i / 8;
            size_t bitPos = i % 8;
            char byteValue = block[byteIndex];
            _values.emplace_back((bool)((byteValue & (1 << bitPos)) != 0));
        }
    }

    uint64_t Size() const override { return _values.size(); }

    bool BoolValue(uint64_t i) const override { return Value(i); }

    bool ToVector(std::shared_ptr<std::vector<bool>>& input) override { input = std::make_shared<std::vector<bool>>(_values); return true; }

private:
    std::vector<bool> _values;
};

class Int32Array : public Array {
public:
    uint32_t Value(uint64_t i) const {
        if (i > _values.size()) {
            throw std::runtime_error("Array index out of bounds.");
        }
        return _values[i];
    }
    void StoreBlock(char* block, size_t numValues) {
        auto size = sizeof(uint32_t);
        size_t offset = 0;
        for (size_t i = 0; i < numValues; ++i) {
            _values.emplace_back(*(uint32_t*)(block + offset));
            offset += size;
        }
    }

    uint64_t Size() const override { return _values.size(); }

    uint32_t Int32Value(uint64_t i) const override { return Value(i); }

    bool ToVector(std::shared_ptr<std::vector<uint32_t>>& input) override { input = std::make_shared<std::vector<uint32_t>>(_values); return true; }

private:
    std::vector<uint32_t> _values;
};

class Int64Array : public Array {
public:
    uint64_t Value(uint64_t i) const {
        if (i > _values.size()) {
            throw std::runtime_error("Array index out of bounds.");
        }
        return _values[i];
    }
    void StoreBlock(char* block, size_t numValues) {
        auto size = sizeof(uint64_t);
        size_t offset = 0;
        for (size_t i = 0; i < numValues; ++i) {
            _values.emplace_back(*(uint64_t*)(block + offset));
            offset += size;
        }
    }

    uint64_t Size() const override { return _values.size(); }

    uint64_t Int64Value(uint64_t i) const override { return Value(i); }

    bool ToVector(std::shared_ptr<std::vector<uint64_t>>& input) override { input = std::make_shared<std::vector<uint64_t>>(_values); return true; }

private:
    std::vector<uint64_t> _values;
};

class FloatArray : public Array {
public:
    float Value(uint64_t i) const {
        if (i > _values.size()) {
            throw std::runtime_error("Array index out of bounds.");
        }
        return _values[i];
    }
    void StoreBlock(char* block, size_t numValues) {
        auto size = sizeof(float);
        size_t offset = 0;
        for (size_t i = 0; i < numValues; ++i) {
            _values.emplace_back(*(float*)(block + offset));
            offset += size;
        }
    }

    uint64_t Size() const override { return _values.size(); }

    float FloatValue(uint64_t i) const override { return Value(i); }

    bool ToVector(std::shared_ptr<std::vector<float>>& input) override { input = std::make_shared<std::vector<float>>(_values); return true; }

private:
    std::vector<float> _values;
};

class DoubleArray : public Array {
public:
    double Value(uint64_t i) const {
        if (i > _values.size()) {
            throw std::runtime_error("Array index out of bounds.");
        }
        return _values[i];
    }
    void StoreBlock(char* block, size_t numValues) {
        auto size = sizeof(double);
        size_t offset = 0;
        for (size_t i = 0; i < numValues; ++i) {
            _values.emplace_back(*(double*)(block + offset));
            offset += size;
        }
    }

    uint64_t Size() const override { return _values.size(); }

    double DoubleValue(uint64_t i) const override { return Value(i); }

    bool ToVector(std::shared_ptr<std::vector<double>>& input) override { input = std::make_shared<std::vector<double>>(_values); return true; }

private:
    std::vector<double> _values;
};

class StrArray : public Array {
public:
    const std::string& Value(uint64_t i) const {
        if (i > _values.size()) {
            throw std::runtime_error("Array index out of bounds.");
        }
        return _values[i];
    }
    void Store(std::string& val) {
        _values.emplace_back(val);
    }

    uint64_t Size() const override { return _values.size(); }

    std::string StrValue(uint64_t i) const override { return Value(i); }

    bool ToVector(std::shared_ptr<std::vector<std::string>>& input) override { input = std::make_shared<std::vector<std::string>>(_values); return true; }

private:
    std::vector<std::string> _values;
};
} // end namespace clearParquet
