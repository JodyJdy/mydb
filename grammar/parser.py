from typing import Type

from antlr4.tree.Tree import TerminalNodeImpl

from sql_ast import *
from SQLiteParser import *

from grammar.sql_ast import CurrentTime, TrueLiteral, StringLiteral, NumericLiteral, NullLiteral, FalseLiteral, \
    CurrentDate


def parse_expr(context: SQLiteParser.ExprContext) -> Expr:
    # 提前解析一些表达式
    first_expr_context: SQLiteParser.ExprContext = context.expr(0)
    second_expr_context: SQLiteParser.ExprContext = context.expr(1) if first_expr_context else None
    third_expr_context: SQLiteParser.ExprContext = context.expr(2) if second_expr_context else None
    first_expr = parse_expr(first_expr_context) if first_expr_context else None
    second_expr = parse_expr(second_expr_context) if second_expr_context else None
    third_expr = parse_expr(third_expr_context) if third_expr_context else None

    if context.literal_value():
        return parse_literal_value(context.literal_value())
    elif context.BIND_PARAMETER():
        text = get_text(context.BIND_PARAMETER())
        if '?' in text:
            return BindParameter(True, text[1:])
        else:
            return BindParameter(False, text[1:])
    elif context.column_name():
        schema_name = process_context_with_any_name(context.schema_name())
        table_name = process_context_with_any_name(context.table_name())
        column = process_context_with_any_name(context.column_name())
        return ColumnName(schema_name, table_name, column)
    elif context.unary_operator():
        return parse_unary_expr(context)
    elif context.PIPE2():
        return BinaryExpr(BinaryOperator.PIPE2, first_expr, second_expr)
    elif context.STAR():
        return BinaryExpr(BinaryOperator.STAR, first_expr, second_expr)
    elif context.DIV():
        return BinaryExpr(BinaryOperator.DIV, first_expr, second_expr)
    elif context.MOD():
        return BinaryExpr(BinaryOperator.MOD, first_expr, second_expr)
    elif context.PLUS():
        return BinaryExpr(BinaryOperator.PLUS, first_expr, second_expr)
    elif context.MINUS():
        return BinaryExpr(BinaryOperator.MINUS, first_expr, second_expr)
    elif context.LT2():
        return BinaryExpr(BinaryOperator.LT2, first_expr, second_expr)
    elif context.GT2():
        return BinaryExpr(BinaryOperator.GT2, first_expr, second_expr)
    elif context.AMP():
        return BinaryExpr(BinaryOperator.AMP, first_expr, second_expr)
    elif context.PIPE():
        return BinaryExpr(BinaryOperator.PIPE, first_expr, second_expr)
    elif context.LT():
        return BinaryExpr(BinaryOperator.LT, first_expr, second_expr)
    elif context.LT_EQ():
        return BinaryExpr(BinaryOperator.LT_EQ, first_expr, second_expr)
    elif context.GT_EQ():
        return BinaryExpr(BinaryOperator.GT_EQ, first_expr, second_expr)
    elif context.ASSIGN():
        return BinaryExpr(BinaryOperator.ASSIGN, first_expr, second_expr)
    elif context.EQ():
        return BinaryExpr(BinaryOperator.EQ, first_expr, second_expr)
    elif context.NOT_EQ1():
        return BinaryExpr(BinaryOperator.NOT_EQ1, first_expr, second_expr)
    elif context.NOT_EQ2():
        return BinaryExpr(BinaryOperator.NOT_EQ2, first_expr, second_expr)
    elif context.IS_():
        not_ = context.NOT_() is not None
        distinct = context.DISTINCT_() is not None
        return IsExpr(not_, first_expr, second_expr, distinct)
    elif context.LIKE_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.LIKE, first_expr, second_expr, third_expr if escape else None)
    elif context.GLOB_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.GLOB, first_expr, second_expr, third_expr if escape else None)
    elif context.REGEXP_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.REGEX, first_expr, second_expr, third_expr if escape else None)
    elif context.MATCH_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.MATCH, first_expr, second_expr, third_expr if escape else None)
    elif context.AND_():
        return BinaryExpr(BinaryOperator.AND, first_expr, second_expr)
    elif context.OR_():
        return BinaryExpr(BinaryOperator.OR, first_expr, second_expr)
    elif context.function_name():
        func_name = process_any_name_context(context.function_name().any_name)
        distinct = context.DISTINCT_() is not None
        star = context.STAR() is not None
        params = [parse_expr(context) for context in context.expr()]
        return FuncCall(func_name, distinct, star, params)
    elif context.CAST_():
        type_name = get_type_name(context.type_name())
        return Cast(first_expr, type_name)
    elif context.collation_name():
        collation_context: SQLiteParser.Collation_nameContext = context.collation_name()
        collation_name = process_any_name_context(collation_context.any_name())
        return ExprWithCollate(first_expr, collation_name)
    elif context.ISNULL_():
        return IsNullExpr(first_expr, False)
    elif context.NOTNULL_() or context.NULL_():
        return IsNullExpr(first_expr, True)
    elif context.BETWEEN_():
        return BetweenExpr(first_expr, second_expr, context.NOT_() is not None)
    elif context.IN_():
        negative = context.NOT_() is not None
        result: InExpr
        if context.select_stmt():
            # return SelectInExpr
            result = None
        elif context.table_function_name():
            schema = process_context_with_any_name(context.schema_name())
            table_func_name = process_any_name_context(context.table_function_name().any_name())
            params = [parse_expr(context) for context in context.expr()[1:]]
            result = TableFuncNameInExpr(first_expr, schema, table_func_name, params)
        elif context.table_name():
            schema = process_context_with_any_name(context.schema_name())
            table_name = process_any_name_context(context.table_name())
            result = TableNameInExpr(first_expr, schema, table_name)
        elif context.OPEN_PAR():
            in_exprs = [parse_expr(context) for context in context.expr()[1:]]
            result = ExprsInExpr(first_expr, in_exprs)
        else:
            result = SingleExprInExpr(first_expr, second_expr)
        result.negative = negative
        return result
    elif context.select_stmt():
        select: SelectStmt
        if context.NOT_():
            return SelectExpr(True, select)
        else:
            return SelectExpr(False, select)
    elif context.CASE_():
        return None
    return None

def process_context_with_any_name(context):
    if not context:
        return None
    return process_any_name_context(context.any_name())

def process_any_name_context(any_name: SQLiteParser.Any_nameContext):
    if any_name.IDENTIFIER():
        return get_text(any_name.IDENTIFIER())
    if any_name.keyword():
        return get_text(any_name.keyword().getChild(0))
    if any_name.STRING_LITERAL():
        return get_text(any_name.STRING_LITERAL())
    if any_name.any_name():
        return process_any_name_context(any_name.any_name())


def parse_signed_number(signed: SQLiteParser.Signed_numberContext):
    if not signed:
        return None
    value = parse_numeric(signed.NUMERIC_LITERAL())
    if signed.MINUS():
        value = - value
    return value


def get_type_name(type_name_context: SQLiteParser.Type_nameContext) -> TypeName:
    name_context_list: List[SQLiteParser.NameContext] = type_name_context.name()
    name_list = [process_any_name_context(l.any_name()) for l in name_context_list]
    name = str.join('', name_list)
    num_a = parse_signed_number(type_name_context.signed_number(0))
    num_b = parse_signed_number(type_name_context.signed_number(1))
    type_name = TypeName(name, num_a, num_b)
    return type_name


def get_text(terminal: TerminalNodeImpl) -> str:
    if not terminal:
        return ''
    if not isinstance(terminal, TerminalNodeImpl):
        return ''
    return terminal.symbol.text


def parse_numeric(numeric: SQLiteParser.NUMERIC_LITERAL):
    text = get_text(numeric)
    if '0x' in text or '0X' in text:
        text = text[2:]
        return int(text, 16)
    return float(text)


def parse_numeric_literal(numeric: SQLiteParser.NUMERIC_LITERAL) -> NumericLiteral:
    return NumericLiteral(parse_numeric(numeric))


def parse_literal_value(context: SQLiteParser.Literal_valueContext) -> Expr:
    if context.NUMERIC_LITERAL():
        return parse_numeric_literal(context.NUMERIC_LITERAL())
    elif context.STRING_LITERAL():
        text = get_text(context.STRING_LITERAL())[1:-1]
        return StringLiteral(text)
    elif context.BLOB_LITERAL():
        text = get_text(context.BLOB_LITERAL())[2:-1]
        return StringLiteral(text)
    elif context.NULL_():
        return NullLiteral()
    elif context.TRUE_():
        return TrueLiteral()
    elif context.FALSE_():
        return FalseLiteral()
    elif context.CURRENT_TIME_():
        return CurrentTime()
    elif context.CURRENT_DATE_():
        return CurrentDate()
    return CurrentTimeStamp()


def parse_unary_expr(context: SQLiteParser.ExprContext) -> Expr:
    operator = get_text(context.unary_operator())
    expr = parse_expr(context.expr(0))
    if '-' == operator:
        return UnaryExpr(UnaryOperator.MINUS, expr)
    elif '+' == operator:
        return UnaryExpr(UnaryOperator.PLUS, expr)
    elif '~' == operator:
        return UnaryExpr(UnaryOperator.TILDE, expr)
    else:
        return UnaryExpr(UnaryOperator.NOT, expr)


def parse_stmt_list(stmt_list: SQLiteParser.Sql_stmt_listContext) -> StmtList:
    return StmtList([parse_stmt(stmt) for stmt in stmt_list.sql_stmt()])


def parse_stmt(stmt_context: SQLiteParser.Sql_stmtContext) -> Stmt:
    explain = stmt_context.EXPLAIN_() is not None
    query_plan = stmt_context.QUERY_() is not None
    stmt: Stmt = do_parse_stmt(stmt_context)
    stmt.explain = explain
    stmt.query_plan = query_plan
    return stmt


def do_parse_stmt(stmt_context: SQLiteParser.Sql_stmtContext) -> Stmt:
    if stmt_context.alter_table_stmt():
        return parse_alter_table_stmt(stmt_context.alter_table_stmt())
    elif stmt_context.analyze_stmt():
        return parse_analyze_stmt(stmt_context.analyze_stmt())
    elif stmt_context.attach_stmt():
        return parse_attach_stmt(stmt_context.attach_stmt())
    elif stmt_context.begin_stmt():
        return parse_begin_stmt(stmt_context.begin_stmt())
    elif stmt_context.commit_stmt():
        return parse_commit()
    return None

def parse_asc_desc(asc_desc : SQLiteParser.Asc_descContext):
    if not asc_desc:
        return None
    if asc_desc.ASC_():
        return True

def parse_conflict_clause(conflict_clause : SQLiteParser.Conflict_clauseContext)->ConflictClause:
    if conflict_clause.ROLLBACK_():
        return ConflictClause.ROLLBACK
    elif conflict_clause.ABORT_():
        return ConflictClause.ABORT
    elif conflict_clause.FAIL_():
        return ConflictClause.FAIL
    elif conflict_clause.IGNORE_():
        return ConflictClause.IGNORE
    elif conflict_clause.REPLACE_():
        return ConflictClause.REPLACE

def parse_foreign_key_clause(name:str,foreign:SQLiteParser.Foreign_key_clauseContext)->ForeignKeyClause:
    table_name = process_context_with_any_name(foreign.foreign_table())
    column_name =[ process_context_with_any_name(c)for c in foreign.column_name()]
    on_clause:List[ForeignKeyOnClause] = []
    i = 0
    while i < len(foreign.children):
        if 'ON' == str.upper(get_text(foreign.children[i])):
            is_delete ='DELETE'== str.upper( get_text(foreign.children[i+1]))
            if "SET" == str.upper(get_text(foreign.children[i+2])):
                action = str.upper(get_text(foreign.children[i+3]))
                i = i + 4
            else:
                action = str.upper(get_text(foreign.children[i+2]))
                i = i + 3
            if "NULL" == action:
                on_clause.append(ForeignKeyOnClause.DELETE_SET_NULL  if is_delete else ForeignKeyOnClause.UPDATE_SET_NULL)
            elif "DEFAULT" == action:
                on_clause.append(ForeignKeyOnClause.DELETE_SET_DEFAULT  if is_delete else ForeignKeyOnClause.UPDATE_SET_DEFAULT)
            elif "CASCADE" == action:
                on_clause.append(ForeignKeyOnClause.DELETE_CASCADE  if is_delete else ForeignKeyOnClause.UPDATE_CASCADE)
            elif "RESTRICT" == action:
                on_clause.append(ForeignKeyOnClause.DELETE_RESTRICT  if is_delete else ForeignKeyOnClause.UPDATE_RESTRICT)
            elif "NO" == action:
                on_clause.append(ForeignKeyOnClause.DELETE_NO_ACTION  if is_delete else ForeignKeyOnClause.UPDATE_NO_ACTION)
        else:
            i = i + 1
    if foreign.NOT_() is None and foreign.DEFERRABLE_() and foreign.DEFERRED_():
        immediate = False
    else:
        immediate = True

    return ForeignKeyClause(name,table_name,column_name,on_clause,immediate)


def parse_column_constraint(constraint :SQLiteParser.Column_constraintContext)->ColumnConstraint:
    name = process_any_name_context(constraint.name().any_name()) if constraint.name().any_name() else constraint.name()
    isAsc = parse_asc_desc(constraint.asc_desc())
    conflict_clause = parse_conflict_clause(constraint.conflict_clause())

    if constraint.PRIMARY_():
        autoincrement = constraint.AUTOINCREMENT_() is not None
        return PrimaryKeyConstraint(name,isAsc,conflict_clause,autoincrement)
    elif constraint.NULL_():
        return NullConstraint(name,constraint.NOT_() is not None)
    elif constraint.UNIQUE_():
        return UniqueConstraint(name)
    elif constraint.CHECK_():
        expr = parse_expr(constraint.expr())
        return CheckConstraint(name,expr)
    elif constraint.DEFAULT_():
        if constraint.signed_number():
            expr = parse_signed_number(constraint.signed_number())
        elif constraint.literal_value():
            expr = parse_literal_value(constraint.literal_value())
        else:
            expr = parse_expr(constraint.expr())
        return DefaultConstraint(name,expr)
    elif constraint.collation_name():
        collation_name = process_any_name_context(constraint.collation_name().any_name())
        return CollateConstraint(name,collation_name)
    elif constraint.AS_():
        generate_always = constraint.GENERATED_() is not None
        stored = constraint.STORED_() is not None
        expr = parse_expr(constraint.expr())
        return AsConstraint(name,generate_always,expr,stored)
    else:
        return parse_foreign_key_clause(name,constraint.foreign_key_clause())




def parse_column_def(context:SQLiteParser.Column_defContext)->ColumnDef:
    column_name = process_context_with_any_name(context.column_name())
    type_name = get_type_name(context.type_name())if context.type_name() else None
    constraint_list:List[ColumnConstraint] = []
    for constraint in context.column_constraint():
        constraint_list.append(parse_column_constraint(constraint))
    return ColumnDef(column_name,type_name,constraint_list)

def parse_alter_table_stmt(context: SQLiteParser.Alter_table_stmtContext) -> AlterTable:
    schema_name = process_context_with_any_name(context.schema_name())
    table_name = process_context_with_any_name(context.table_name(0))
    if context.new_table_name:
        return AlterTableRenameTable(schema_name,table_name,process_context_with_any_name(context.new_table_name))
    elif context.old_column_name:
        return AlterTableRenameColumn(schema_name,table_name,process_context_with_any_name(context.old_column_name),process_context_with_any_name(context.new_column_name))
    elif context.column_name(0):
        return AlterTableDropColumn(schema_name,table_name,process_context_with_any_name(context.column_name()))
    else:
        return AlterTableAddColumn(schema_name,table_name,parse_column_def(context.column_def()))


def parse_analyze_stmt(context:SQLiteParser.Analyze_stmtContext)->AnalyzeStmt:
    schema_name = process_context_with_any_name(context.schema_name())
    table_or_index_name = process_context_with_any_name(context.table_or_index_name())
    return AnalyzeStmt(schema_name,table_or_index_name)

def parse_attach_stmt(attach:SQLiteParser.Attach_stmtContext)->AttachStmt:
    data_base = attach.DATABASE_() is not None
    expr = parse_expr(attach.expr())
    schema_name = process_context_with_any_name(attach.schema_name())
    return AttachStmt(data_base,expr,schema_name)


def parse_begin_stmt(begin:SQLiteParser.Begin_stmtContext)->BeginStmt:
    if begin.DEFERRED_():
        begin_type = TransactionBeginType.DEFERRED
    elif begin.IMMEDIATE_():
        begin_type = TransactionBeginType.IMMEDIATE
    elif begin.EXCLUSIVE_():
        begin_type = TransactionBeginType.EXCLUSIVE
    transaction_name = process_context_with_any_name(begin.transaction_name())
    return BeginStmt(begin_type,transaction_name)
def parse_commit()->CommitStmt:
    return CommitStmt()

