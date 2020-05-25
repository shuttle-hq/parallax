use super::ValidateResult;

use sqlparser::ast;

macro_rules! ansatz {
    (
        $id:ident:
        $($f:ident($o:path),)*
    ) => {
        #[derive(Clone)]
        pub enum $id {
            $($f($o),)*
        }

        $(
            impl From<$o> for $id {
                fn from(v: $o) -> Self {
                    Self::$f(v)
                }
            }

            impl Into<$o> for $id {
                fn into(self) -> $o {
                    match self {
                        Self::$f(v) => v,
                        _ => <Self as Into<$o>>::into(self.next())
                    }
                }
            }
        )*
    }
}

ansatz! { RelAnsatz:
TableFactor(ast::TableFactor),
TableWithJoins(ast::TableWithJoins),
Select(ast::Select),
SetExpr(ast::SetExpr),
Query(ast::Query), }

impl RelAnsatz {
    fn next(self) -> Self {
        match self {
            Self::TableFactor(table_factor) => Self::TableWithJoins(ast::TableWithJoins {
                relation: table_factor,
                joins: vec![],
            }),
            Self::TableWithJoins(table_with_joins) => Self::Select(ast::Select {
                distinct: false,
                projection: vec![ast::SelectItem::Wildcard],
                from: vec![table_with_joins],
                selection: None,
                group_by: vec![],
                having: None,
            }),
            Self::Select(select) => Self::SetExpr(ast::SetExpr::Select(Box::new(select))),
            Self::Query(query) => Self::TableFactor(ast::TableFactor::Derived {
                lateral: false,
                subquery: Box::new(query),
                alias: None,
            }),
            Self::SetExpr(set_expr) => Self::Query(ast::Query {
                ctes: vec![],
                body: set_expr,
                order_by: vec![],
                limit: None,
                offset: None,
                fetch: None,
            }),
        }
    }
    pub fn with_alias(self, alias: &str) -> Self {
        let mut from: ast::TableFactor = self.into();
        let t_alias = ast::TableAlias {
            name: alias.to_string(),
            columns: vec![],
        };
        match &mut from {
            ast::TableFactor::Derived { alias, .. } | ast::TableFactor::Table { alias, .. } => {
                *alias = Some(t_alias);
                from.into()
            }
            ast::TableFactor::NestedJoin(..) => Self::from(from).next().with_alias(alias),
        }
    }
    pub fn wrapped<T>(self) -> T
    where
        Self: Into<T>,
    {
        self.next().into()
    }
}

ansatz! { ExprAnsatz:
Expr(ast::Expr),
SelectItem(ast::SelectItem), }

impl ExprAnsatz {
    fn next(self) -> Self {
        match self {
            Self::Expr(expr) => Self::SelectItem(match expr {
                ast::Expr::QualifiedWildcard(ids) => {
                    ast::SelectItem::QualifiedWildcard(ast::ObjectName(ids))
                }
                ast::Expr::Wildcard => ast::SelectItem::Wildcard,
                _ => ast::SelectItem::UnnamedExpr(expr),
            }),
            Self::SelectItem(select_item) => Self::Expr(match select_item {
                ast::SelectItem::ExprWithAlias { expr, .. }
                | ast::SelectItem::UnnamedExpr(expr) => expr,
                ast::SelectItem::QualifiedWildcard(ids) => ast::Expr::QualifiedWildcard(ids.0),
                ast::SelectItem::Wildcard => ast::Expr::Wildcard,
            }),
        }
    }
    pub fn with_alias(self, alias: &str) -> Self {
        Self::SelectItem(ast::SelectItem::ExprWithAlias {
            expr: self.into(),
            alias: alias.to_string(),
        })
    }
    pub fn get_alias(&self) -> Option<ast::Ident> {
        match self {
            Self::SelectItem(ast::SelectItem::ExprWithAlias { alias, .. }) => Some(alias.clone()),
            _ => None,
        }
    }
    pub fn map_inside<F>(self, f: F) -> Self
    where
        F: FnOnce(ast::Expr) -> ast::Expr,
    {
        let alias = self.get_alias();
        let expr: ast::Expr = self.into();
        let out: Self = f(expr).into();
        if let Some(alias) = alias {
            out.with_alias(&alias)
        } else {
            out
        }
    }
}

pub trait ToAnsatz {
    type Ansatz;
    fn to_ansatz(self) -> Result<Self::Ansatz, CompositionError>;
}

#[derive(Debug, Clone)]
pub enum CompositionError {
    Unimplemented,
}

impl std::fmt::Display for CompositionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unimplemented => write!(f, "Unimplemented"),
        }
    }
}
