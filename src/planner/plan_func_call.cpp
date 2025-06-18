/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-06-17 20:43:08
 * @FilePath: /CMU-15-445/src/planner/plan_func_call.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include <memory>
#include <tuple>
#include "binder/bound_expression.h"
#include "binder/bound_statement.h"
#include "binder/expressions/bound_agg_call.h"
#include "binder/expressions/bound_alias.h"
#include "binder/expressions/bound_binary_op.h"
#include "binder/expressions/bound_column_ref.h"
#include "binder/expressions/bound_constant.h"
#include "binder/expressions/bound_func_call.h"
#include "binder/expressions/bound_unary_op.h"
#include "binder/statement/select_statement.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/util/string_util.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/string_expression.h"
#include "execution/plans/abstract_plan.h"
#include "fmt/format.h"
#include "planner/planner.h"

namespace bustub {

auto Planner::PlanFuncCall(const BoundFuncCall &expr, const std::vector<AbstractPlanNodeRef> &children)
    -> AbstractExpressionRef {
  std::vector<AbstractExpressionRef> args;
  for (const auto &arg : expr.args_) {
    auto [_1, arg_expr] = PlanExpression(*arg, children);
    args.push_back(std::move(arg_expr));
  }
  return GetFuncCallFromFactory(expr.func_name_, std::move(args));
}

// NOLINTNEXTLINE
auto Planner::GetFuncCallFromFactory(const std::string &func_name, std::vector<AbstractExpressionRef> args)
    -> AbstractExpressionRef {
  // 1. check if the parsed function name is "lower" or "upper".
  // 2. verify the number of args (should be 1), refer to the test cases for when you should throw an `Exception`.
  // 3. return a `StringExpression` std::shared_ptr.
  // throw Exception(fmt::format("func call {} not supported in planner yet", func_name));

  //—————————————————————————————————start————————————————————————————————————
  // 1.检查解析的函数名是“lower”还是“upper”
  // 2.验证args的数量（应该是1），参考测试用例了解何时应该抛出“Exception”。
  // 3.返回一个`StringExpression ` std:：shared_ptr。

  if (args.size() != 1) {
    throw Exception(ExceptionType::INVALID, "lower function requires exactly one argument");
  }
  if (args[0]->GetReturnType().GetType() != TypeId::VARCHAR) {
    throw Exception(ExceptionType::INVALID, "unexpected arg");
  }

  if (func_name == "lower") {
    return std::make_shared<StringExpression>(args[0], StringExpressionType::Lower);
  }
  if (func_name == "upper") {
    return std::make_shared<StringExpression>(args[0], StringExpressionType::Upper);
  }

  throw Exception(ExceptionType::UNKNOWN_TYPE, fmt::format("Unknown function call: {}", func_name));

  //—————————————————————————————————end————————————————————————————————————
}

}  // namespace bustub
