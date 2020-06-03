use std::convert::{TryFrom, TryInto};

use sqlparser::{
    ast::{
        self, Expr, Query, Select, SelectItem, SetExpr, Statement, TableAlias, TableFactor,
        TableWithJoins,
    },
    dialect::GenericDialect,
    parser::{Parser as SqlParser, ParserError},
};

use super::*;

fn maybe_wrap_alias(rel_t: RelT, alias: Option<&TableAlias>) -> ValidateResult<RelT> {
    if let Some(alias) = alias.as_ref() {
        if alias.columns.len() != 0 {
            // FIXME
            Err(ValidateError::Wip(
                "parsing column aliases on top of a table alias".to_string(),
            ))
        } else {
            Ok(RelT::from(WithAlias {
                from: rel_t,
                alias: alias.name.clone(),
            }))
        }
    } else {
        Ok(rel_t)
    }
}

pub trait Validate: ToContext<M = TableMeta> {
    fn validate_query(&self, query: &Query) -> ValidateResult<RelT> {
        let mut root = self.validate_set_expr(&query.body)?;

        let ctx = self.to_context().flatten();
        let expr_validator = ExprValidator::new(&ctx);

        if query.order_by.len() > 0 {
            let mut orders = Vec::new();
            let mut exprs = Vec::new();
            for ob in query.order_by.iter() {
                orders.push(match ob.asc.unwrap_or(false) {
                    true => Order::Asc,
                    false => Order::Desc,
                });
                exprs.push(expr_validator.validate_expr(&ob.expr)?);
            }
            root = RelT::from(OrderBy {
                order: orders,
                by: exprs,
                from: root,
            });
        }

        if let Some(offset) = query.offset.as_ref() {
            let offset = expr_validator.validate_expr(offset)?;
            root = RelT::from(Offset { offset, from: root });
        }

        if let Some(number_rows) = query.limit.as_ref() {
            let number_rows = expr_validator.validate_expr(number_rows)?;
            root = RelT::from(Limit {
                number_rows,
                from: root,
            });
        }

        Ok(root)
    }

    fn validate_set_expr(&self, set_expr: &SetExpr) -> ValidateResult<RelT> {
        match set_expr {
            SetExpr::Select(select) => self.validate_select(select),
            SetExpr::Query(subquery) => self.validate_query(subquery),
            SetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => Ok(RelT::from(Set {
                operator: op.into(),
                left: self.validate_set_expr(left)?,
                right: self.validate_set_expr(right)?,
            })),
            SetExpr::Values(values) => Err(ValidateError::Wip(
                "parsing literal arrays in set expressions".to_string(),
            )),
        }
    }

    fn validate_select(&self, select: &Select) -> ValidateResult<RelT> {
        let mut from = select
            .from
            .iter()
            .map(|slc| self.validate_table_with_joins(slc))
            .collect::<ValidateResult<Vec<RelT>>>()?;

        if from.len() > 1 {
            return Err(ValidateError::Wip("SELECT [..] FROM (array)".to_string()));
        }

        let from = from
            .pop()
            .ok_or(ValidateError::Wip("SELECT [..] EOF".to_string()))?;

        let ctx = from.try_to_context()?;
        let expr_validator = ExprValidator::new(&ctx);

        let mut select_items = Vec::<ExprT>::new();
        for si in select.projection.iter() {
            let expr_t: Vec<ExprT> = expr_validator.validate_select_item(si)?;
            select_items.extend(expr_t);
        }

        let mut root: RelT;

        if !select.group_by.is_empty() {
            let group_by = select
                .group_by
                .iter()
                .map(|gb| expr_validator.validate_expr(gb))
                .collect::<ValidateResult<Vec<_>>>()?;
            root = RelT::from(Aggregation {
                attributes: select_items,
                group_by,
                from,
            });
        } else {
            root = RelT::from(Projection {
                attributes: select_items,
                from,
            });
        }

        if let Some(slc) = &select.selection {
            let where_ = ExprValidator::new(&root.try_to_context()?).validate_expr(slc)?;
            root = RelT::from(Selection { from: root, where_ });
        }

        if select.distinct {
            root = RelT::from(Distinct { from: root });
        }

        Ok(root)
    }

    fn validate_table_factor(&self, table_factor: &TableFactor) -> ValidateResult<RelT> {
        match table_factor {
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let sub_t = self.validate_query(subquery)?;
                maybe_wrap_alias(sub_t, alias.as_ref())
            }
            TableFactor::Table {
                name: ast::ObjectName(name),
                alias,
                ..
            } => {
                if name.len() != 2 {
                    let name_str = name.as_slice().join(".");
                    Err(ValidateError::InvalidTableName(name_str))
                } else {
                    let dataset_name = name[0].as_str();
                    let table_name = name[1].as_str();
                    let key = ContextKey::with_name(table_name).and_prefix(dataset_name);
                    let table_meta = self.to_context().get_table(&key).map(|m| m.clone());
                    let rel_t = RelT {
                        root: Table(key).into(),
                        board: table_meta,
                    };
                    maybe_wrap_alias(rel_t, alias.as_ref())
                }
            }
            TableFactor::NestedJoin(table_with_joins) => {
                self.validate_table_with_joins(table_with_joins)
            }
        }
    }

    fn validate_table_with_joins(&self, table_with_joins: &TableWithJoins) -> ValidateResult<RelT> {
        let mut left = self.validate_table_factor(&table_with_joins.relation)?;

        let mut ctx = left.try_to_context()?;

        for join in table_with_joins.joins.iter() {
            let right = self.validate_table_factor(&join.relation)?;
            ctx.extend(right.try_to_context()?);

            let expr_validator = ExprValidator::new(&ctx);

            let m_jc = match &join.join_operator {
                ast::JoinOperator::Inner(cst)
                | ast::JoinOperator::LeftOuter(cst)
                | ast::JoinOperator::RightOuter(cst)
                | ast::JoinOperator::FullOuter(cst) => {
                    Some(expr_validator.validate_join_constraint(cst)?)
                }
                _ => None,
            };

            let operator = match &join.join_operator {
                ast::JoinOperator::Inner(_) => JoinOperator::Inner(m_jc.unwrap()),
                ast::JoinOperator::LeftOuter(_) => JoinOperator::LeftOuter(m_jc.unwrap()),
                ast::JoinOperator::RightOuter(_) => JoinOperator::RightOuter(m_jc.unwrap()),
                ast::JoinOperator::FullOuter(_) => JoinOperator::FullOuter(m_jc.unwrap()),
                ast::JoinOperator::CrossJoin => JoinOperator::CrossJoin,
                _ => {
                    return Err(ValidateError::NotSupported(
                        "`CrossJoin` or `OuterApply` join constraints".to_string(),
                    ))
                }
            };

            left = RelT::from(Join {
                left,
                right,
                operator,
            });
        }

        Ok(left)
    }
}

pub struct Validator<'a> {
    ctx: &'a Context<TableMeta>,
}

impl<'a> ToContext for Validator<'a> {
    type M = TableMeta;
    fn to_context(&self) -> Context<Self::M> {
        self.ctx.clone()
    }
}

impl<'a> Validator<'a> {
    pub fn new(ctx: &'a Context<TableMeta>) -> Self {
        Self { ctx }
    }

    pub fn validate_str(&self, sql: &str) -> ValidateResult<RelT> {
        let dialect = GenericDialect {}; // ANSI SQL11
        let mut sql_statements = SqlParser::parse_sql(&dialect, sql.to_string())?;

        if sql_statements.len() > 1 {
            Err(ValidateError::MultipleStatementsNotAllowed)
        } else {
            let stmt = sql_statements.pop().ok_or(ValidateError::EmptyRequest)?;
            match stmt {
                Statement::Query(query) => self.validate_query(query.as_ref()),
                _ => Err(ValidateError::NotAQuery(stmt.to_string())),
            }
        }
    }
}

impl<'a> Validate for Validator<'a> {}

pub trait ValidateExpr: ToContext<M = ExprMeta> {
    fn validate_expr(&self, expr: &Expr) -> ValidateResult<ExprT> {
        match expr {
            Expr::Identifier(ident) => {
                let key = ContextKey::with_name(ident.as_str());
                let expr_meta = self.to_context().get_column(&key).map(|m| m.clone());
                Ok(ExprT {
                    root: Column(key).into(),
                    board: expr_meta,
                })
            }
            Expr::Wildcard | Expr::QualifiedWildcard(..) => {
                // We only allow wildcards in top-level expr of SelectItem
                Err(ValidateError::UnexpectedWildcard)
            }
            Expr::CompoundIdentifier(c_ident) => {
                if c_ident.len() > 2 || c_ident.len() == 0 {
                    let id_str = if c_ident.len() == 0 {
                        "EMPTY".to_string()
                    } else {
                        c_ident.as_slice().join(".")
                    };
                    Err(ValidateError::InvalidIdentifier(format!(
                        "compound `{}`",
                        id_str
                    )))
                } else {
                    // FIXME: efficiency
                    let mut c_ident = c_ident.clone();
                    c_ident.reverse();
                    let mut key = ContextKey::with_name(&c_ident[0]);
                    if let Some(rel) = c_ident.get(1) {
                        key = key.and_prefix(rel);
                    }
                    let expr_meta = self.to_context().get_column(&key).map(|m| m.clone());
                    Ok(ExprT {
                        root: Column(key).into(),
                        board: expr_meta,
                    })
                }
            }
            Expr::IsNull(expr) => Ok(ExprT::from(IsNull(self.validate_expr(expr)?))),
            Expr::IsNotNull(expr) => Ok(ExprT::from(IsNotNull(self.validate_expr(expr)?))),
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = self.validate_expr(expr)?;
                let list = list
                    .iter()
                    .map(|expr| self.validate_expr(expr))
                    .collect::<ValidateResult<Vec<_>>>()?;
                Ok(ExprT::from(InList {
                    expr,
                    list,
                    negated: *negated,
                }))
            }
            Expr::Between {
                expr,
                negated,
                low,
                high,
            } => {
                let expr = self.validate_expr(expr)?;
                let low = self.validate_expr(low)?;
                let high = self.validate_expr(high)?;
                Ok(ExprT::from(Between {
                    expr,
                    negated: *negated,
                    low,
                    high,
                }))
            }
            Expr::BinaryOp { left, op, right } => {
                let left = self.validate_expr(left)?;
                let right = self.validate_expr(right)?;
                Ok(ExprT::from(BinaryOp {
                    left,
                    op: op.into(),
                    right,
                }))
            }
            Expr::Function(ast::Function {
                name,
                args,
                over,
                distinct,
            }) => {
                let name = FunctionName::try_from(name)?;
                let args = args
                    .iter()
                    .map(|e| self.validate_expr(e))
                    .collect::<ValidateResult<Vec<_>>>()?;
                Ok(ExprT::from(Function {
                    name,
                    args,
                    distinct: *distinct,
                }))
            }
            Expr::Value(value) => {
                let lit = value.try_into()?;
                Ok(ExprT::from(Literal(lit)))
            }
            _ => Err(ValidateError::Wip(
                "not all expression variants are currently supported".to_string(),
            )),
        }
    }

    fn validate_select_item(&self, select_item: &SelectItem) -> ValidateResult<Vec<ExprT>> {
        match select_item {
            SelectItem::UnnamedExpr(expr) => {
                // TODO: FUNCTION(*)
                self.validate_expr(expr).map(|res| vec![res])
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let expr = self.validate_expr(expr)?;
                Ok(vec![ExprT::from(As {
                    expr,
                    alias: alias.clone(),
                })])
            }
            SelectItem::Wildcard => self
                .to_context()
                .iter()
                .map(|(k, m)| {
                    Ok(ExprT {
                        root: Column(k.clone()).into(),
                        board: Ok(m.clone()),
                    })
                })
                .collect(),
            SelectItem::QualifiedWildcard(on) => {
                let on: ContextKey = on.clone().try_into()?;
                self.to_context()
                    .iter()
                    .filter_map(|(k, m)| if k.matches(&on) { Some((k, m)) } else { None })
                    .map(|(key, expr_meta)| {
                        Ok(ExprT {
                            root: Column(key.clone()).into(),
                            board: Ok(expr_meta.clone()),
                        })
                    })
                    .collect()
            }
        }
    }

    fn validate_join_constraint(
        &self,
        join_constraint: &ast::JoinConstraint,
    ) -> ValidateResult<JoinConstraint> {
        match join_constraint {
            ast::JoinConstraint::On(expr) => Ok(JoinConstraint::On(self.validate_expr(&expr)?)),
            ast::JoinConstraint::Using(keys) => {
                let context_keys: Vec<_> = keys.iter().map(|k| ContextKey::with_name(&k)).collect();

                if context_keys.is_empty() {
                    return Err(ValidateError::Expected(
                        "`USING` join constraint to be followed by at least one column ident"
                            .to_string(),
                    ));
                }

                Ok(JoinConstraint::Using(context_keys))
            }
            ast::JoinConstraint::Natural => Ok(JoinConstraint::Natural),
        }
    }
}

pub struct ExprValidator<'a> {
    ctx: &'a Context<ExprMeta>,
}

impl<'a> ExprValidator<'a> {
    fn new(ctx: &'a Context<ExprMeta>) -> Self {
        Self { ctx }
    }
}

impl<'a> ToContext for ExprValidator<'a> {
    type M = ExprMeta;
    fn to_context(&self) -> Context<ExprMeta> {
        self.ctx.clone()
    }
}

impl<'a> ValidateExpr for ExprValidator<'a> {}

#[cfg(test)]
pub mod tests {
    use super::*;

    use tokio::runtime::Runtime;

    use crate::node::tests::just_get_me_a_context as get_context;

    #[test]
    fn rebase_relation_tree() {
        let original_tree = test_validate_for(
            "SELECT ethnicity_concept_id as ethnicity_concept_id, \
                     COUNT(DISTINCT person_id) \
             FROM (\
               SELECT ethnicity_concept_id as ethnicity_concept_id, person_id as person_id \
               FROM patient_data.person \
             )\
             GROUP BY ethnicity_concept_id",
        );
        let mut new_ctx = get_context();
        let person_meta = new_ctx
            .get_mut(&ContextKey::with_name("person").and_prefix("patient_data"))
            .unwrap();
        let ethn_id_meta = person_meta
            .columns
            .get_mut(&ContextKey::with_name("ethnicity_concept_id"))
            .unwrap();
        ethn_id_meta.ty = DataType::Integer;
        let new_ctx_ref = &new_ctx;
        let rebase_fut = rebase_closure!(original_tree => TableMeta -> TableMeta {
            async move |_, key| {
                new_ctx_ref
                    .get(key)
                    .map(|v| v.clone())
                    .map_err(|e| e.into_table_error())
            }
        });
        let rebased_tree = Runtime::new().unwrap().block_on(rebase_fut);
        assert_eq!(
            rebased_tree
                .board
                .unwrap()
                .columns
                .get(&ContextKey::with_name("ethnicity_concept_id"))
                .unwrap()
                .ty,
            DataType::Integer
        );
    }

    pub fn test_validate_for(query: &str) -> RelT {
        let ctx = get_context();
        let validator = Validator::new(&ctx);
        let rel_t = validator.validate_str(query).unwrap();
        rel_t
    }

    #[test]
    fn validate_group_by() {
        crate::tests::setup_test();
        let rel_t = test_validate_for(
            "\
            SELECT race_concept_id, COUNT(person_id) \
            FROM patient_data.person \
            GROUP BY race_concept_id",
        );
        let table_meta = rel_t.board.unwrap();
        let expr_ctx = table_meta.to_context();
        let expr_meta = expr_ctx.get(&"f1_".parse().unwrap()).unwrap();
        assert_eq!(expr_meta.ty, DataType::Integer)
    }

    #[test]
    fn validate_alias() {
        let rel_t = test_validate_for("SELECT person_id AS person FROM patient_data.person");
        let expr_ctx = rel_t.board.unwrap().to_context();
        expr_ctx.get(&"person".parse().unwrap()).unwrap();
    }

    #[test]
    fn validate_join() {
        let rel_t = test_validate_for(
            "\
            SELECT a.race_concept_id, COUNT(DISTINCT a.person_id)
            FROM patient_data.person AS a
            JOIN patient_data.location AS b
            ON a.location_id = b.location_id
            GROUP BY a.race_concept_id
            ",
        );
        let expr_meta = rel_t.board.unwrap().to_context();
    }
}
