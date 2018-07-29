/*
 * storage_engine.cpp
 * @author Kevin Lundeen
 * This file implements a coule of new Value operations which
 * didnt have a home elsewhere
 */

// we moved DbRelation::project(ValueDict where) to be a member
// of HeapTable

#include "common.h"

bool Value::operator==(const Value &other) const {
    if (this->data_type != other.data_type)
        return false;
    if (this->data_type == ColumnAttribute::INT)
        return this->n == other.n;
    return this->s == other.s;
}

bool Value::operator!=(const Value &other) const {
    return !(*this == other);
}

