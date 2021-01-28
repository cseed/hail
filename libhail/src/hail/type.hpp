#ifndef HAIL_TYPE_HPP_INCLUDED
#define HAIL_TYPE_HPP_INCLUDED 1

#include <map>

#include <hail/allocators.hpp>

namespace hail {

class PType;
class FormatStream;
class TypeContext;

class TypeContextToken {
  friend class TypeContext;
  TypeContextToken() {}
};

class Type {
public:
  using BaseType = Type;
  enum class Tag {
    VOID,
    BOOL,
    INT32,
    INT64,
    FLOAT32,
    FLOAT64,
    STR,
    ARRAY,
    STREAM,
    TUPLE
  };
  const Tag tag;
  Type(Tag tag) : tag(tag) {}
  virtual ~Type();

  bool is_realizable() const {
    return (tag != Tag::VOID) && (tag != Tag::STREAM);
  }
};

void format1(FormatStream &s, const Type *v);

class TVoid : public Type {
public:
  static const Tag self_tag = Type::Tag::VOID;
  TVoid(TypeContextToken) : Type(self_tag) {}
};

class TBool : public Type {
public:
  static const Tag self_tag = Type::Tag::BOOL;
  TBool(TypeContextToken) : Type(self_tag) {}
};

class TInt32 : public Type {
public:
  static const Tag self_tag = Type::Tag::INT32;
  TInt32(TypeContextToken) : Type(self_tag) {}
};

class TInt64 : public Type {
public:
  static const Tag self_tag = Type::Tag::INT64;
  TInt64(TypeContextToken) : Type(self_tag) {}
};

class TFloat32 : public Type {
public:
  static const Tag self_tag = Type::Tag::FLOAT32;
  TFloat32(TypeContextToken) : Type(self_tag) {}
};

class TFloat64 : public Type {
public:
  static const Tag self_tag = Type::Tag::FLOAT64;
  TFloat64(TypeContextToken) : Type(self_tag) {}
};

class TStr : public Type {
public:
  static const Tag self_tag = Type::Tag::STR;
  TStr(TypeContextToken) : Type(self_tag) {}
};

class TArray : public Type {
public:
  static const Tag self_tag = Type::Tag::ARRAY;
  const Type *const element_type;
  TArray(TypeContextToken, const Type *element_type) : Type(self_tag), element_type(element_type) {}
};

class TStream : public Type {
public:
  static const Tag self_tag = Type::Tag::STREAM;
  const Type *const element_type;
  TStream(TypeContextToken, const Type *element_type) : Type(self_tag), element_type(element_type) {}
};

class TTuple : public Type {
public:
  static const Tag self_tag = Type::Tag::TUPLE;
  const std::vector<const Type *> element_types;
  TTuple(TypeContextToken, std::vector<const Type *> element_types) : Type(self_tag), element_types(std::move(element_types)) {}
};

class TypeContext {
  ArenaAllocator arena;

  std::map<const Type *, const TArray *> arrays;
  std::map<const Type *, const TStream *> streams;
  std::map<std::vector<const Type *>, const TTuple *> tuples;

  std::map<const Type *, const PType *> canonical_ptypes;

public:
  const TVoid *const tvoid;
  const TBool *const tbool;
  const TInt32 *const tint32;
  const TInt64 *const tint64;
  const TFloat32 *const tfloat32;
  const TFloat64 *const tfloat64;
  const TStr *const tstr;

  TypeContext(HeapAllocator &heap);

  const TArray *tarray(const Type *element_type);
  const TStream *tstream(const Type *element_type);
  const TTuple *ttuple(const std::vector<const Type *> &element_types);

  const PType *get_canonical_ptype(const Type *type);
};

}

#endif
