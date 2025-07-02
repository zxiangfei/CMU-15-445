/*
 * @Author: zxiangfei 2464257291@qq.com
 * @Date: 2025-06-15 15:26:01
 * @LastEditors: zxiangfei 2464257291@qq.com
 * @LastEditTime: 2025-07-01 20:08:15
 * @FilePath: /CMU-15-445/src/optimizer/seqscan_as_indexscan.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
#include "optimizer/optimizer.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/index_scan_plan.h"

namespace bustub {

/**
 * 实现将SeqScanPlanNode优化为IndexScanPlanNode的优化器规则
 * 
 * 只有一种情况可以把顺序的变成索引的，即以下条件同时满足:
 * * 谓词（即where之后的语句）不为空
 * * 表支持索引扫描
 * * 只有一个谓词条件（如SELECT * FROM t1 WHERE v1 = 1 AND v2 = 2就不行）
 * * 谓词是等值条件（即WHERE v1 = 1）
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  
  //对所有子结点递归应用这一优化
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSeqScanAsIndexScan(child));
  }

  //将当前结点的子结点替换为优化后的子结点
  auto new_plan = plan->CloneWithChildren(std::move(children));

  //检查当前结点是否为顺序扫描计划节点
  if (new_plan->GetType() == PlanType::SeqScan) {  //顺序扫描计划，继续执行
    const auto &seq_scan_plan = dynamic_cast<const bustub::SeqScanPlanNode &>(*new_plan); //转为 SeqScanPlanNode 类型

    //检查谓词是否存在
    auto filter_predicate = seq_scan_plan.filter_predicate_;
    if (filter_predicate != nullptr) {  //谓词存在，继续执行
      //获取表的索引，检查该表是否支持索引扫描
      auto table_indexes = catalog_.GetTableIndexes(seq_scan_plan.table_name_);

      //将谓词转化为逻辑组合表达式，用于判断是否是逻辑组合谓词
      auto logic_filter_predicate = std::dynamic_pointer_cast<LogicExpression>(filter_predicate);
      if(!table_indexes.empty() && !logic_filter_predicate) {  //表支持索引扫描且谓词不是逻辑组合谓词
        auto equal_filter_predicate = std::dynamic_pointer_cast<ComparisonExpression>(filter_predicate);  //将谓词转为单一比较表达式
  
        if(equal_filter_predicate){  //是单一表达式
          //检查逻辑谓词是否是等值条件
          auto com_type = equal_filter_predicate->comp_type_;
          if (com_type == ComparisonType::Equal) {  //是等值条件,才能转为索引扫描
            //获取表的id
            auto table_oid = seq_scan_plan.table_oid_;

            //获取索引扫描列
            auto columns_expr = dynamic_cast<const ColumnValueExpression &>(*equal_filter_predicate->GetChildAt(0));  //获取比较表达式的左侧子表达式
            auto columns_index = columns_expr.GetColIdx();  //通过左侧表达式获取列的索引
            auto columns_name = catalog_.GetTable(table_oid)->schema_.GetColumn(columns_index).GetName();  //通过列索引获取列名

            //遍历相关索引，找到与列名匹配的索引
            for (const auto &index_info : table_indexes) {
              const auto &columns = index_info->index_->GetKeyAttrs(); //获取遍历的当前索引是建立在哪些列上的
              if(columns == std::vector<uint32_t>{columns_index}) {  //如果当前索引的列与比较表达式的列匹配
                //找到索引后，获取谓词表达式右侧的常量值，构造IndexScanPlanNode替换SeqScan
                //获取 ConstantValueExpression类型的 pred_key
                auto pred_key = std::dynamic_pointer_cast<ConstantValueExpression>(equal_filter_predicate->GetChildAt(1));
                ConstantValueExpression *pred_key_ptr = pred_key ? pred_key.get() : nullptr;  //如果转换成功，获取指针，否则为nullptr

                //创建 IndexScanPlanNode 替换 SeqScanPlanNode
                return std::make_shared<IndexScanPlanNode>(
                    seq_scan_plan.output_schema_,
                    table_oid,
                    index_info->index_oid_,
                    filter_predicate,  //使用原谓词
                    pred_key_ptr  //传入常量值表达式
                );
              }
            }
          }
        }

      }
    }
  }

  return new_plan;
}

}  // namespace bustub
