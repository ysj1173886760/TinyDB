/**
 * @file comparison_expression.cpp
 * @author sheep
 * @brief comparison type
 * @version 0.1
 * @date 2022-05-19
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "execution/expressions/comparison_expression.h"
#include "type/value_factory.h"
#include "common/exception.h"

namespace TinyDB {

Value ComparisonExpression::Evaluate(const Tuple *tuple_left, const Tuple *tuple_right) const {
    TINYDB_ASSERT(tuple_left != nullptr || tuple_right != nullptr, "null tuple");
    // TINYDB_ASSERT(tuple_right != nullptr, "null tuple");
    // intuitively, we only need to pass tuple_left to left child, and pass tuple_right to right child
    // i'm not sure which one is better
    // sheep: actually, tuple_left and tuple_right doesn't mean the tuple used by left child. It actually means 
    // at any 2-ary operator, tuple_left will be the lhs parameter, and tuple_right will be the rhs parameter
    // let's take an example, suppose we have an expression for conditional join: 
    // TableA join TableB on TableA.a > 10 and TableB.b > 20 and TableA.a == TableB.b
    // so we will have 2 tuple as input to evaluate the tuple(one is from TableA, and another is from TableB)
    // then we can form an abstraction expression tree like this:
    //              Conjunction_And
    //              /             | 
    //          Comp_GT         Conjunction_And
    //         /      |         /             | 
    //     TupleA     10      Comp_LT       Comp_EQ
    //                       /      |       /     | 
    //                      20   TupleB   TupleA  TupleB
    // Note that i've changed TableB.b > 20 to 20 < TableB.b. So every tuple that from TableA is in lhs,
    // and tuple from TableB is in rhs. Thus we can pass tuple from TableA as "tuple_left" and tuple from TableB
    // as "tuple_right". 
    // Again, key point is that tuple_left is not the tuple passed to left child. It's actually the tuple at lhs(left hand side)
    // when performing 2-ary operation.
    // !!! But actual Abstraction Expression Tree is not like this one, because i've simplified some nodes. Thus i need to change
    // TableB.b > 20 to 20 < TableB.b to place TableB at right size. In the real implementation, the tree should be like this:
    //              Conjunction_And
    //              /              |  
    //          Comp_GT           Conjunction_And
    //         /      |           /             | 
    //     Column    10         Comp_GT       Comp_EQ
    //    /     |              /      |       /     |  
    //   A      B            Column  20   Column    Column
    //                       /   |        /   |      /   |  
    //                      A    B       A    B     A    B
    // Now you see the difference, ColumnValueExpression will help us to extract the value we need.
    // for the 1-ary operation, we just ignore one parameter.
    // for the n-ary operation(n > 2), we can exploit the associative property of set operation

    auto val_left = children_[0]->Evaluate(tuple_left, tuple_right);
    auto val_right = children_[1]->Evaluate(tuple_left, tuple_right);
    // should we check whether they are comparable first?
    switch (type_) {
    case ExpressionType::ComparisonExpression_Equal:
        return ValueFactory::GetBooleanValue(val_left.CompareEquals(val_right));
    case ExpressionType::ComparisonExpression_NotEqual:
        return ValueFactory::GetBooleanValue(val_left.CompareNotEquals(val_right));
    case ExpressionType::ComparisonExpression_GreaterThan:
        return ValueFactory::GetBooleanValue(val_left.CompareGreaterThan(val_right));
    case ExpressionType::ComparisonExpression_GreaterThanEquals:
        return ValueFactory::GetBooleanValue(val_left.CompareGreaterThanEquals(val_right));
    case ExpressionType::ComparisonExpression_LessThan:
        return ValueFactory::GetBooleanValue(val_left.CompareLessThan(val_right));
    case ExpressionType::ComparisonExpression_LessThanEquals:
        return ValueFactory::GetBooleanValue(val_left.CompareLessThanEquals(val_right));
    default:
        THROW_LOGIC_ERROR_EXCEPTION("invalid expression type");
    }
}

}