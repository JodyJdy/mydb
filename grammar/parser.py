
from antlr4.tree.Tree import TerminalNodeImpl

from grammar.SQLiteParser import *

from grammar.sql_ast import *



def parse_expr(context: SQLiteParser.ExprContext) -> Expr|None:
    if not context:
        return None
    # 提前解析一些表达式
    first_expr_context: SQLiteParser.ExprContext = context.expr(0)
    second_expr_context: SQLiteParser.ExprContext = context.expr(1) if first_expr_context else None
    third_expr_context: SQLiteParser.ExprContext = context.expr(2) if second_expr_context else None
    first_expr = parse_expr(first_expr_context) if first_expr_context else None
    second_expr = parse_expr(second_expr_context) if second_expr_context else None
    third_expr = parse_expr(third_expr_context) if third_expr_context else None
    not_ = context.NOT_() is not None

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
    elif context.GT():
        return BinaryExpr(BinaryOperator.GT, first_expr, second_expr)
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
        distinct = context.DISTINCT_() is not None
        return IsExpr(not_, first_expr, second_expr, distinct)
    elif context.LIKE_():
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.LIKE, first_expr, second_expr, third_expr if escape else None)
    elif context.GLOB_():
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.GLOB, first_expr, second_expr, third_expr if escape else None)
    elif context.REGEXP_():
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.REGEX, first_expr, second_expr, third_expr if escape else None)
    elif context.MATCH_():
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_, TextMatchType.MATCH, first_expr, second_expr, third_expr if escape else None)
    elif context.AND_():
        return BinaryExpr(BinaryOperator.AND, first_expr, second_expr)
    elif context.OR_():
        return BinaryExpr(BinaryOperator.OR, first_expr, second_expr)
    elif context.function_name():
        func_name = process_context_with_any_name(context.function_name())
        distinct = context.DISTINCT_() is not None
        star = context.STAR() is not None
        params = [parse_expr(context) for context in context.expr()]
        return FuncCall(func_name, distinct, star, params)
    elif context.CAST_():
        type_name = get_type_name(context.type_name())
        return Cast(first_expr, type_name)
    elif context.collation_name():
        collation_context: SQLiteParser.Collation_nameContext = context.collation_name()
        collation_name = process_context_with_any_name(collation_context)
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
            table_func_name = process_context_with_any_name(context.table_function_name())
            params = [parse_expr(context) for context in context.expr()[1:]]
            result = TableFuncNameInExpr(first_expr, schema, table_func_name, params)
        elif context.table_name():
            schema = process_context_with_any_name(context.schema_name())
            table_name = process_context_with_any_name(context.table_name())
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
        return SelectExpr(not_, select)
    elif context.CASE_():
        exprs = [parse_expr(context) for context in context.expr()]
        case:Expr = None
        when_then:List[(Expr, Expr)] = []
        else_expr:Expr = None
        # case expr
        if isinstance(context.children[1],SQLiteParser.ExprContext):
            case = exprs[0]
            exprs = exprs[1:]
        if context.ELSE_():
            else_expr = exprs[-1]
            exprs =exprs[:-2]
        # 处理 WHEN THEN
        i = 0
        while i < len(exprs):
            when_then.append((exprs[i],exprs[i+1]))
            i+=2
        return CaseThenExpr(case,when_then,else_expr)
    return None

def process_context_with_alias_name(context):
    if not context:
        return None
    return process_alias_name_context(context.alias_name())

def process_context_with_any_name(context):
    if not context:
        return None
    return process_any_name_context(context.any_name())

def process_alias_name_context(alias_name: SQLiteParser.Alias_nameContext):
    if alias_name.IDENTIFIER():
        return get_text(alias_name.IDENTIFIER())
    if alias_name.STRING_LITERAL():
        return get_text(alias_name.STRING_LITERAL())
    if alias_name.alias_name():
        return process_alias_name_context(alias_name.alias_name())

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
    name_list = [process_context_with_any_name(l) for l in name_context_list]
    name = str.join('', name_list)
    num_a = parse_signed_number(type_name_context.signed_number(0))
    num_b = parse_signed_number(type_name_context.signed_number(1))
    type_name = TypeName(name, num_a, num_b)
    return type_name

def get_children_text(context:ParserRuleContext,list:List[str])->List[str]:
    children = context.children
    result:List[str] = []
    for c in children:
        t = get_text(c)
        if t in list:
            result.append(t)
    return result

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
    elif stmt_context.create_index_stmt():
        return parse_create_index_stmt(stmt_context.create_table_stmt())
    elif stmt_context.create_table_stmt():
        return parse_create_table_stmt(stmt_context.create_table_stmt())
    elif stmt_context.create_trigger_stmt():
        return parse_create_trigger_stmt(stmt_context.create_trigger_stmt())
    elif stmt_context.create_view_stmt():
        return parse_create_view_stmt(stmt_context.create_view_stmt())
    elif stmt_context.delete_stmt():
        return parse_delete_stmt(stmt_context.delete_stmt())
    elif stmt_context.delete_stmt_limited():
        return parse_delete_stmt_limited(stmt_context.delete_stmt())
    elif stmt_context.detach_stmt():
        return parse_detach_stmt(stmt_context.detach_stmt())
    elif stmt_context.drop_stmt():
        return parse_delete_stmt(stmt_context.drop_stmt())
    elif stmt_context.insert_stmt():
        return parse_insert_stmt(stmt_context.insert_stmt())
    elif stmt_context.reindex_stmt():
        return parse_reindex_stmt(stmt_context.reindex_stmt())
    elif stmt_context.release_stmt():
        return parse_release_stmt(stmt_context.release_stmt())
    elif stmt_context.rollback_stmt():
        return parse_release_stmt(stmt_context.rollback_stmt())
    elif stmt_context.savepoint_stmt():
        return parse_savepoint_stmt(stmt_context.savepoint_stmt())
    elif stmt_context.select_stmt():
        return parse_select_stmt(stmt_context.select_stmt())
    elif stmt_context.update_stmt():
        return parse_update_stmt(stmt_context.update_stmt())
    elif stmt_context.update_stmt_limited():
        return parse_update_stmt_limited(stmt_context.update_stmt_limited())
    raise Exception('not implemented')


def parse_asc_desc(asc_desc: SQLiteParser.Asc_descContext):
    if not asc_desc:
        return None
    if asc_desc.ASC_():
        return True
    return False


def parse_conflict_clause(conflict_clause: SQLiteParser.Conflict_clauseContext) -> ConflictClause:
    if conflict_clause.ROLLBACK_():
        return ConflictClause.ROLLBACK
    elif conflict_clause.ABORT_():
        return ConflictClause.ABORT
    elif conflict_clause.FAIL_():
        return ConflictClause.FAIL
    elif conflict_clause.IGNORE_():
        return ConflictClause.IGNORE
    else:
        return ConflictClause.REPLACE


def parse_foreign_key_clause(name: str, foreign: SQLiteParser.Foreign_key_clauseContext) -> ForeignKeyClause:
    table_name = process_context_with_any_name(foreign.foreign_table())
    column_name = [process_context_with_any_name(c) for c in foreign.column_name()]
    on_clause: List[ForeignKeyOnClause] = []
    i = 0
    while i < len(foreign.children):
        if 'ON' == str.upper(get_text(foreign.children[i])):
            is_delete = 'DELETE' == str.upper(get_text(foreign.children[i + 1]))
            if "SET" == str.upper(get_text(foreign.children[i + 2])):
                action = str.upper(get_text(foreign.children[i + 3]))
                i = i + 4
            else:
                action = str.upper(get_text(foreign.children[i + 2]))
                i = i + 3
            action_type = ForeignKeyActionType.parse_action(action)
            on_clause.append(ForeignKeyOnClause(is_delete,action_type))
        else:
            i = i + 1
    if foreign.NOT_() is None and foreign.DEFERRABLE_() and foreign.DEFERRED_():
        immediate = False
    else:
        immediate = True

    return ForeignKeyClause(name, table_name, column_name, on_clause, immediate)


def parse_column_constraint(constraint: SQLiteParser.Column_constraintContext) -> ColumnConstraint:
    name = process_any_name_context(constraint.name().any_name()) if constraint.name().any_name() else constraint.name()
    isAsc = parse_asc_desc(constraint.asc_desc())
    conflict_clause = parse_conflict_clause(constraint.conflict_clause())

    if constraint.PRIMARY_():
        autoincrement = constraint.AUTOINCREMENT_() is not None
        return PrimaryKeyConstraint(name, isAsc, conflict_clause, autoincrement)
    elif constraint.NULL_():
        return NullConstraint(name, constraint.NOT_() is not None)
    elif constraint.UNIQUE_():
        return UniqueConstraint(name)
    elif constraint.CHECK_():
        expr = parse_expr(constraint.expr())
        return CheckConstraint(name, expr)
    elif constraint.DEFAULT_():
        if constraint.signed_number():
            expr = parse_signed_number(constraint.signed_number())
        elif constraint.literal_value():
            expr = parse_literal_value(constraint.literal_value())
        else:
            expr = parse_expr(constraint.expr())
        return DefaultConstraint(name, expr)
    elif constraint.collation_name():
        collation_name = process_context_with_any_name(constraint.collation_name())
        return CollateConstraint(name, collation_name)
    elif constraint.AS_():
        generate_always = constraint.GENERATED_() is not None
        stored = constraint.STORED_() is not None
        expr = parse_expr(constraint.expr())
        return AsConstraint(name, generate_always, expr, stored)
    else:
        return parse_foreign_key_clause(name, constraint.foreign_key_clause())


def parse_column_def(context: SQLiteParser.Column_defContext) -> ColumnDef:
    column_name = process_context_with_any_name(context.column_name())
    type_name = get_type_name(context.type_name()) if context.type_name() else None
    constraint_list: List[ColumnConstraint] = []
    for constraint in context.column_constraint():
        constraint_list.append(parse_column_constraint(constraint))
    return ColumnDef(column_name, type_name, constraint_list)


def parse_alter_table_stmt(context: SQLiteParser.Alter_table_stmtContext) -> AlterTable:
    schema_name = process_context_with_any_name(context.schema_name())
    table_name = process_context_with_any_name(context.table_name(0))
    if context.new_table_name:
        return AlterTableRenameTable(schema_name, table_name, process_context_with_any_name(context.new_table_name))
    elif context.old_column_name:
        return AlterTableRenameColumn(schema_name, table_name, process_context_with_any_name(context.old_column_name),
                                      process_context_with_any_name(context.new_column_name))
    elif context.column_name(0):
        return AlterTableDropColumn(schema_name, table_name, process_context_with_any_name(context.column_name()))
    else:
        return AlterTableAddColumn(schema_name, table_name, parse_column_def(context.column_def()))


def parse_analyze_stmt(context: SQLiteParser.Analyze_stmtContext) -> AnalyzeStmt:
    schema_name = process_context_with_any_name(context.schema_name())
    table_or_index_name = process_context_with_any_name(context.table_or_index_name())
    return AnalyzeStmt(schema_name, table_or_index_name)


def parse_attach_stmt(attach: SQLiteParser.Attach_stmtContext) -> AttachStmt:
    data_base = attach.DATABASE_() is not None
    expr = parse_expr(attach.expr())
    schema_name = process_context_with_any_name(attach.schema_name())
    return AttachStmt(data_base, expr, schema_name)


def parse_begin_stmt(begin: SQLiteParser.Begin_stmtContext) -> BeginStmt:
    begin_type = None
    if begin.DEFERRED_():
        begin_type = TransactionBeginType.DEFERRED
    elif begin.IMMEDIATE_():
        begin_type = TransactionBeginType.IMMEDIATE
    elif begin.EXCLUSIVE_():
        begin_type = TransactionBeginType.EXCLUSIVE
    transaction_name = process_context_with_any_name(begin.transaction_name())
    return BeginStmt(begin_type, transaction_name)


def parse_commit() -> CommitStmt:
    return CommitStmt()

def parse_indexed_column(context:SQLiteParser.Indexed_columnContext):
    column_name = process_context_with_any_name(context.column_name())
    expr = parse_expr(context.expr())
    collation_name = process_context_with_any_name(context.collation_name())
    isAsc = parse_asc_desc(context.asc_desc())
    if column_name:
        return ColumnIndexedColumn(column_name,collation_name,isAsc)
    return ExprIndexedColumn(expr,collation_name,isAsc)

def parse_create_index_stmt(context:SQLiteParser.Create_index_stmtContext)->CreateIndexStmt:
    unique = context.UNIQUE_() is not None
    if_not_exist = context.IF_() is not None
    index_name = process_context_with_any_name(context.index_name())
    schema_name = process_context_with_any_name(context.schema_name())
    table_name = process_context_with_any_name(context.table_name())
    indexed_columns = [parse_indexed_column(c) for c in context.indexed_column()]
    where = parse_expr(context.expr())
    return CreateIndexStmt(unique,if_not_exist,schema_name, index_name,table_name, indexed_columns, where)

def parse_table_constraint(context :SQLiteParser.Table_constraintContext):
    name = process_context_with_any_name(context.name())
    if context.PRIMARY_() or context.UNIQUE_():
        indexed_column = [parse_indexed_column(c) for c in context.indexed_column()]
        if context.PRIMARY_():
            return PrimaryTableConstraint(name,indexed_column)
        return UniqueTableConstraint(name,indexed_column)
    elif context.CHECK_():
        return CheckTableConstraint(name,parse_expr(context.expr()))
    else:
        column_names = [ process_context_with_any_name(c)for c in context.column_name()]
        foreign_clause = parse_foreign_key_clause(context.foreign_key_clause())
        return ForeignKeyOnTableConstraint(name,column_names,foreign_clause)

def parse_create_table_stmt(context:SQLiteParser.Create_table_stmtContext):
    temp = context.TEMP_() is not None or context.TEMPORARY_() is not None
    if_not_exist = context.IF_() is not None
    schema_name = process_context_with_any_name(context.schema_name())
    table_name = process_context_with_any_name(context.table_name())
    if context.select_stmt():
        #todo
        return SelectCreateTableStmt(temp,if_not_exist,schema_name,table_name,None)
    column_defs = [parse_column_def(c) for c in context.column_def()]
    context.table_constraint()
    table_constraints = [parse_table_constraint(c) for c in context.table_constraint()]
    return UsuallyCreateTableStmt(temp,if_not_exist,schema_name,table_name,table_constraints,column_defs)

def get_children_by_type(context:ParserRuleContext,types : Tuple)->List[any]:
    children = context.children
    result = []
    for c in children:
        if isinstance(c,types):
            result.append(c)
    return result


def parse_create_trigger_stmt(context:SQLiteParser.Create_trigger_stmtContext):
    trigger_stmt = CreateTriggerStmt()
    trigger_stmt.temp = context.TEMP_() or context.TEMPORARY_()
    trigger_stmt.if_not_exist = context.NOT_()
    trigger_stmt.schema_name = process_context_with_any_name(context.schema_name())
    trigger_stmt.trigger_name = process_context_with_any_name(context.trigger_name())
    trigger_time:TriggerTimeEnum = None
    if context.BEFORE_():
        trigger_time = TriggerTimeEnum.BEFORE
    elif context.AFTER_():
        trigger_time = TriggerTimeEnum.AFTER
    elif context.INSTEAD_():
        trigger_time = TriggerTimeEnum.INSTEAD_OF
    trigger_type :TriggerStmtTypeEnum = None
    if context.DELETE_():
        trigger_type = TriggerStmtTypeEnum.DELETE
    elif context.INSERT_():
        trigger_type = TriggerStmtTypeEnum.INSERT
    elif context.UPDATE_():
        trigger_type = TriggerStmtTypeEnum.UPDATE
        trigger_stmt.update_column_names = [process_context_with_any_name(c)for c in context.column_name()]
    trigger_stmt.trigger_type = trigger_type
    trigger_stmt.trigger_time = trigger_time
    trigger_stmt.table_name = process_context_with_any_name(context.table_name())
    if context.FOR_():
        trigger_stmt.for_each_row = True
    if context.WHEN_():
        trigger_stmt.when = parse_expr(context.expr())
        context.update_stmt()
    parser = context.parser
    crud_context = get_children_by_type(context,( parser.Select_stmtContext,parser.Insert_stmtContext,parser.Delete_stmtContext,parser.Update_stmtContext))
    trigger_stmt.stmt_list =  [parse_stmt(c)for c in crud_context]
    return trigger_stmt
def parse_create_view_stmt(context:SQLiteParser.Create_view_stmtContext):
    view_stmt = CreateViewStmt()
    view_stmt.temp = context.TEMP_() or context.TEMPORARY_()
    view_stmt.if_not_exist = context.IF_() is not None
    view_stmt.schema_name = process_context_with_any_name(context.schema_name())
    view_stmt.column_names = [process_context_with_any_name(c) for c in context.column_name()]
    view_stmt.select_stmt = parse_select_stmt(context.select_stmt())
    return view_stmt
def parse_create_virtual_table_stmt(context:SQLiteParser.Create_virtual_table_stmtContext):
    raise Exception('Not implemented')
def cte_table_name(context:SQLiteParser.Cte_table_nameContext)->CteTableName:
    cte = CteTableName()
    cte.table_name  = process_context_with_any_name(context.table_name())
    cte.column_names = [process_context_with_any_name(c) for c in context.column_name()]
    return cte

def with_clause(context:SQLiteParser.With_clauseContext)->WithClause|None:
    if not context:
        return None
    with_ = WithClause()
    with_.recursive = context.RECURSIVE_() is not None
    cte_list =  [cte_table_name(c) for c in context.cte_table_name()]
    select_list = [parse_select_stmt(c) for c in context.select_stmt()]
    with_.clauses = []
    for c,s in zip(cte_list,select_list):
        with_.clauses.append(WithClauseContent(c,s))
    return with_
def qualified_table_name(context:SQLiteParser.Qualified_table_nameContext):
    qualified = QualifiedTableName()
    qualified.schema_name = process_context_with_any_name(context.schema_name())
    qualified.table_name = process_context_with_any_name(context.table_name())
    qualified.alias_name = process_context_with_alias_name(context.alias())
    qualified.index_name = process_context_with_any_name(context.index_name())
    return qualified
def result_column(column: SQLiteParser.Result_columnContext):
    r = ReturningClause()
    if column.STAR():
        r.star = True
    if column.table_name():
        r.table_name = process_context_with_any_name(column.table_name())
    if column.expr():
        r.expr = parse_expr(column.expr())
        alias: SQLiteParser.Column_aliasContext = column.column_alias()
        if alias:
            if alias.IDENTIFIER():
                r.column_alias = get_text(alias.IDENTIFIER())
            else:
                r.column_alias = get_text(alias.STRING_LITERAL())
    return r

def returning_clause(context:SQLiteParser.Returning_clauseContext):
    return [result_column(c)for c in context.result_column()]
def parse_delete_stmt(context:SQLiteParser.Delete_stmtContext|SQLiteParser.Delete_stmt_limitedContext):
    delete_stmt = DeleteStmt()
    delete_stmt.with_clause = with_clause(context.with_clause())
    delete_stmt.qualified_table_name = qualified_table_name(context.qualified_table_name())
    delete_stmt.expr = parse_expr(context.expr())
    delete_stmt.returning_clauses =returning_clause(context.returning_clause())
    return delete_stmt

def order_by_item(context:SQLiteParser.Ordering_termContext):
    item = OrderingTerm()
    item.expr =parse_expr(context.expr())
    item.is_asc =parse_asc_desc(context.asc_desc())
    item.collation_name = process_context_with_any_name(context.collation_name())
    if context.FIRST_():
        item.null_first = True
    if context.LAST_():
        item.null_first = False
    return item
def order_by(context:SQLiteParser.Order_by_stmtContext):
    if not context:
        return None
    return [order_by_item(o)for o in context.ordering_term()]
def limit(context:SQLiteParser.Limit_stmtContext):
    if not context:
        return None
    l = Limit()
    l.limit = parse_expr(context.expr(0))
    if context.expr(1):
        l.offset = parse_expr(context.expr(1))
    return l

def parse_delete_stmt_limited(context:SQLiteParser.Delete_stmt_limitedContext):
    delete_stmt = parse_delete_stmt(context)
    delete_stmt.limit = limit(context.limit_stmt())
    delete_stmt.order_by = order_by(context.order_by_stmt())
    return delete_stmt
def parse_detach_stmt(context:SQLiteParser.Detach_stmtContext):
    detach = DetachStmt()
    detach.database = context.DATABASE_() is not None
    detach.schema_name = process_context_with_any_name(context.schema_name())
    return detach
def parse_drop_stmt(context:SQLiteParser.Drop_stmtContext):
    drop = DropStmt()
    if context.INDEX_():
        object_type = ObjectTypeEnum.INDEX
    elif context.TABLE_():
        object_type = ObjectTypeEnum.TABLE
    elif context.TRIGGER_():
        object_type = ObjectTypeEnum.TRIGGER
    else:
        object_type = ObjectTypeEnum.VIEW
    drop.object_type = object_type
    if context.IF_():
        drop.if_exist = True
    drop.schema_name = process_context_with_any_name(context.schema_name())
    drop.name = process_any_name_context(context.any_name())
    return drop

def insert_type(context:SQLiteParser.Insert_stmtContext)->InsertTypeEnum:
    if context.ROLLBACK_():
        return InsertTypeEnum.INSERT_OR_ROLLBACK
    elif context.ABORT_():
        return InsertTypeEnum.INSERT_OR_ABORT
    elif context.FAIL_():
        return InsertTypeEnum.INSERT_OR_FAIL
    elif context.IGNORE_():
        return InsertTypeEnum.INSERT_OR_IGNORE
    elif context.REPLACE_():
        if not context.REPLACE_():
            return InsertTypeEnum.REPLACE
        return InsertTypeEnum.INSERT_OR_REPLACE
    else:
        return InsertTypeEnum.INSERT
def value_clause(context:SQLiteParser.Values_clauseContext)->List[List[Expr]]:
    return [[parse_expr(e) for e in row.expr()]for row in  context.value_row()]

def parse_insert_stmt(context:SQLiteParser.Insert_stmtContext):
    insert = InsertStmt()
    insert.insert_type = insert_type(context)
    insert.schema_name = process_context_with_any_name(context.schema_name())
    insert.table_name = process_context_with_any_name(context.table_name())
    insert.table_alias = process_context_with_alias_name(context.table_alias())
    if context.column_name():
        insert.column_names =[process_context_with_any_name(c) for c in context.column_name()]
    if context.values_clause():
        insert.values_clause = value_clause(context.values_clause())
    if context.select_stmt():
        insert.select_stmt = parse_select_stmt(context.select_stmt())
    return insert
def parse_pragma_stmt(context:SQLiteParser.Pragma_stmtContext):
    raise Exception('not implemented')
def parse_reindex_stmt(context:SQLiteParser.Reindex_stmtContext):
    reindex = ReindexStmt()
    reindex.collation_name = process_context_with_any_name(context.collation_name())
    reindex.schema_name = process_context_with_any_name(context.schema_name())
    reindex.table_name = process_context_with_any_name(context.table_name())
    return reindex
def parse_release_stmt(context:SQLiteParser.Release_stmtContext):
    release = ReleaseStmt()
    release.save_point_name = process_context_with_any_name(context.savepoint_name())
    return release

def parse_rollback_stmt(context:SQLiteParser.Rollback_stmtContext):
    rollback = RollbackStmt()
    rollback.save_point_name = process_context_with_any_name(context.savepoint_name())
    return rollback

def parse_savepoint_stmt(context:SQLiteParser.Savepoint_stmtContext):
    savepoint = SavePointStmt()
    savepoint.save_point_name = process_context_with_any_name(context.savepoint_name())
    return savepoint

def table_or_subquery_list(context_list:List[SQLiteParser.Table_or_subqueryContext])->QueryTable:
    # 多个表用逗号分开是 笛卡尔
    query_table_list = [table_or_subquery(c) for c in context_list]
    first = query_table_list[0]
    for table in query_table_list[1:]:
        first = JoinQueryTable(first, table,False,JoinTypeEnum.CARTESIAN)
    return first
def table_or_subquery(context:SQLiteParser.Table_or_subqueryContext)->QueryTable:
    schema_name = process_context_with_any_name(context.schema_name())
    table_alias = process_context_with_alias_name(context.table_alias())
    table_name = process_context_with_any_name(context.table_name())
    if context.table_or_subquery():
        #不只一个
        if context.COMMA():
            return table_or_subquery_list(context.table_or_subquery())
        return table_or_subquery(context.table_or_subquery(0))
    elif context.table_name():
        return SimpleQueryTable(
            schema_name,
            process_context_with_any_name(context.table_name()),
            table_alias,
            table_name
        )
    elif  context.table_function_name():
        params = [parse_expr(c )for c in context.expr()] if context.expr() else []
        return TableFuncQueryTable(
            schema_name,
            process_context_with_any_name(context.table_function_name()),
            params,
            table_alias
        )
    elif context.select_stmt():
        return SelectStmtQueryTable(
            parse_select_stmt(context.select_stmt()),
            table_alias
        )
    elif context.join_clause():
        return join_clause(context.join_clause())
    else:
        raise Exception('not implemented')

def join_operator(context:SQLiteParser.Join_operatorContext)->Tuple[JoinTypeEnum,bool]:
    natural:bool = context.NATURAL_() is not None
    if context.COMMA():
        return JoinTypeEnum.CARTESIAN,natural
    elif context.LEFT_():
        return JoinTypeEnum.LEFT,natural
    elif context.RIGHT_():
        return JoinTypeEnum.RIGHT,natural
    elif context.FULL_():
        return JoinTypeEnum.FULL,natural
    elif context.INNER_():
        return JoinTypeEnum.INNER,natural
    elif context.CROSS_():
        return JoinTypeEnum.CROSS,natural
    else:
        raise Exception('not implemented')

def join_constraint(context: SQLiteParser.Join_constraintContext)->JoinConstraint:
    if context.expr():
        return OnJoinConstraint(parse_expr(context.expr()))
    return UsingJoinConstraint([process_context_with_any_name(c)for c in context.column_name()])

def join_clause(context:SQLiteParser.Join_clauseContext)->JoinQueryTable:
    result = table_or_subquery(context.table_or_subquery(0))
    parser = context.parser
    i = 1
    while i < len(context.children):
        operator,natural = join_operator(context.children[i])
        second = table_or_subquery(context.children[ i + 1 ])
        constraint = None
        i+=2
        if i < len(context.children) and isinstance(context.children[ i ], parser.Join_constraintContext):
            constraint = join_constraint(context.children[ i ])
            i=i+1
        result = JoinQueryTable(result, second,natural, operator, constraint)
    return result

def parse_select_core(context:SQLiteParser.Select_coreContext)->SelectCore:
    if context.values_clause():
        return ValuesClauseSelectCore(value_clause(context.values_clause()))
    select = NormalSelectCore()
    if context.DISTINCT_():
        select.distinct = True
    select.result_columns = [result_column(c) for c in context.result_column()]
    if context.join_clause():
        select.query_table = join_clause(context.join_clause())
    else:
        select.query_table = table_or_subquery_list(context.table_or_subquery())
    if context.whereExpr:
        select.where = parse_expr(context.whereExpr())
    if context.groupByExpr:
        select.group_by = [ parse_expr(c)for c in context.groupByExpr]
    if context.havingExpr:
        select.having = parse_expr(context.havingExpr)
    return select


def parse_select_stmt(context:SQLiteParser.Select_stmtContext):
    o = order_by(context.order_by_stmt())
    l = limit(context.limit_stmt())
    select_core:SelectCore = parse_select_core(context.select_core(0))
    if context.compound_operator():
        i=0
        while i < len(context.compound_operator()):
            operator:SQLiteParser.Compound_operatorContext = context.compound_operator()[i]
            all_ = operator.ALL_() is not None
            select_core = UnionSelectCore(all_,select_core,parse_select_core(context.select_core(i+1)))
            i=i+1
    select_stmt = SelectStmt()
    select_stmt.select_core = select_core
    select_stmt.limit = l
    select_stmt.order_by = o
    return select_stmt

def update_type(context:SQLiteParser.Update_stmtContext|SQLiteParser.Update_stmt_limitedContext)->UpdateTypeEnum:
    if context.ROLLBACK_():
        return UpdateTypeEnum.UPDATE_OR_ROLLBACK
    elif context.ABORT_():
        return UpdateTypeEnum.UPDATE_OR_ABORT
    elif context.REPLACE_():
        return UpdateTypeEnum.UPDATE_OR_REPLACE
    elif context.FAIL_():
        return UpdateTypeEnum.UPDATE_OR_FAIL
    elif context.IGNORE_():
        return UpdateTypeEnum.UPDATE_OR_IGNORE
    else:
        return UpdateTypeEnum.UPDATE
def parse_update_stmt(context:SQLiteParser.Update_stmtContext|SQLiteParser.Update_stmt_limitedContext):
    update_stmt = UpdateStmt()
    update_stmt.update_type = update_type(context)
    update_stmt.qualified_name = qualified_table_name(context.qualified_table_name())
    parser = context.parser
    column_name_type = parser.Column_nameContext
    column_name_list_type = parser.Column_name_listContext
    SQLiteParser.Column_name_listContext.column_name()
    children = get_children_by_type(context,(column_name_type,column_name_list_type))
    set_list:List[UpdateSet] = []
    exprs = [parse_expr(c)for c in context.expr()]
    for i in range(len(children)):
        column_name_or_list = children[i]
        expr = exprs[i]
        if isinstance(column_name_or_list, column_name_type):
            column_name = process_context_with_any_name(column_name_or_list)
            set_list.append(UpdateSet(expr,column_name,None))
        else:
            column_name_list =  [ process_context_with_any_name(c)for c in column_name_or_list.column_name()]
            set_list.append(UpdateSet(expr,None,column_name_list))
    update_stmt.update_set = set_list
    if context.FROM_():
        if context.join_clause():
            update_stmt.from_ = join_clause(context.join_clause())
        else:
            update_stmt.from_ = table_or_subquery_list(context.table_or_subquery())
    if context.WHERE_():
        update_stmt.where = exprs[-1]
    if context.returning_clause():
        update_stmt.return_clauses = returning_clause(context.returning_clause())
    return update_stmt

def parse_update_stmt_limited(context:SQLiteParser.Update_stmt_limitedContext):
    update_stmt = parse_update_stmt(context)
    update_stmt.limit = limit(context.limit_stmt())
    update_stmt.order_by = order_by(context.order_by_stmt())
    return  update_stmt
def parse_vacuum_stmt(context:SQLiteParser.Vacuum_stmtContext):
    raise Exception('not implemented')