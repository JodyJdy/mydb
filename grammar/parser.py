from typing import Type

from antlr4.tree.Tree import TerminalNodeImpl

from sql_ast import *
from SQLiteParser import *

from grammar.sql_ast import CurrentTime, TrueLiteral, StringLiteral, NumericLiteral, NullLiteral, FalseLiteral, \
    CurrentDate


def parse_expr(context: SQLiteParser.ExprContext) -> Expr:
    #提前解析一些表达式
    first_expr_context:SQLiteParser.ExprContext = context.expr(0)
    second_expr_context:SQLiteParser.ExprContext = context.expr(1) if first_expr_context   else None
    third_expr_context:SQLiteParser.ExprContext = context.expr(2) if second_expr_context   else None
    first_expr = parse_expr(first_expr_context) if first_expr_context else None
    second_expr = parse_expr(second_expr_context) if second_expr_context else None
    third_expr = parse_expr(third_expr_context) if third_expr_context else None

    if context.literal_value():
        return parse_literal_value(context.literal_value())
    elif context.BIND_PARAMETER():
        text = get_text(context.BIND_PARAMETER())
        if '?' in text:
            return BindParameter(True,text[1:])
        else:
            return BindParameter(False,text[1:])
    elif context.column_name():
        schema_name = process_schema_name(context.schema_name())
        table_name = process_table_name(context.table_name())
        column = process_column_name(context.column_name())
        return ColumnName(schema_name,table_name,column)
    elif context.unary_operator():
        return parse_unary_expr(context)
    elif context.PIPE2():
        return BinaryExpr(BinaryOperator.PIPE2,first_expr,second_expr)
    elif context.STAR():
        return BinaryExpr(BinaryOperator.STAR,first_expr,second_expr)
    elif context.DIV():
        return BinaryExpr(BinaryOperator.DIV,first_expr,second_expr)
    elif context.MOD():
        return BinaryExpr(BinaryOperator.MOD,first_expr,second_expr)
    elif context.PLUS():
        return BinaryExpr(BinaryOperator.PLUS,first_expr,second_expr)
    elif context.MINUS():
        return BinaryExpr(BinaryOperator.MINUS,first_expr,second_expr)
    elif context.LT2():
        return BinaryExpr(BinaryOperator.LT2,first_expr,second_expr)
    elif context.GT2():
        return BinaryExpr(BinaryOperator.GT2,first_expr,second_expr)
    elif context.AMP():
        return BinaryExpr(BinaryOperator.AMP,first_expr,second_expr)
    elif context.PIPE():
        return BinaryExpr(BinaryOperator.PIPE,first_expr,second_expr)
    elif context.LT():
        return BinaryExpr(BinaryOperator.LT,first_expr,second_expr)
    elif context.LT_EQ():
        return BinaryExpr(BinaryOperator.LT_EQ,first_expr,second_expr)
    elif context.GT_EQ():
        return BinaryExpr(BinaryOperator.GT_EQ,first_expr,second_expr)
    elif context.ASSIGN():
        return BinaryExpr(BinaryOperator.ASSIGN,first_expr,second_expr)
    elif context.EQ():
        return BinaryExpr(BinaryOperator.EQ,first_expr,second_expr)
    elif context.NOT_EQ1():
        return BinaryExpr(BinaryOperator.NOT_EQ1,first_expr,second_expr)
    elif context.NOT_EQ2():
        return BinaryExpr(BinaryOperator.NOT_EQ2,first_expr,second_expr)
    elif context.IS_():
        not_ = context.NOT_() is not None
        distinct = context.DISTINCT_() is not None
        return IsExpr(not_,first_expr,second_expr,distinct)
    elif context.LIKE_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_,TextMatchType.LIKE,first_expr,second_expr,third_expr if escape else None)
    elif context.GLOB_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_,TextMatchType.GLOB,first_expr,second_expr,third_expr if escape else None)
    elif context.REGEXP_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_,TextMatchType.REGEX,first_expr,second_expr,third_expr if escape else None)
    elif context.MATCH_():
        not_ = context.NOT_() is not None
        escape = context.ESCAPE_() is not None
        return TextMatchExpr(not_,TextMatchType.MATCH,first_expr,second_expr,third_expr if escape else None)
    elif context.AND_():
        return BinaryExpr(BinaryOperator.AND,first_expr,second_expr)
    elif context.OR_():
        return BinaryExpr(BinaryOperator.OR,first_expr,second_expr)
    elif context.function_name():
        func_name = process_any_name_context(context.function_name().any_name)
        distinct = context.DISTINCT_() is not None
        star = context.STAR() is not None
        params = [parse_expr(context )for context in context.expr()]
        return FuncCall(func_name,distinct,star,params)
    elif context. CAST_():
        type_name = get_type_name(context.type_name())
        return Cast(first_expr,type_name)
    elif context.collation_name():
        collation_context:SQLiteParser.Collation_nameContext = context.collation_name()
        collation_name = process_any_name_context(collation_context.any_name())
        return ExprWithCollate(first_expr,collation_name)
    elif context.ISNULL_():
        return IsNullExpr(first_expr,False)
    elif context.NOTNULL_() or context.NULL_():
        return IsNullExpr(first_expr,True)
    elif context.BETWEEN_():
        return BetweenExpr(first_expr,second_expr,context.NOT_() is not None)
    elif context.IN_():
        negative = context.NOT_() is not None
        result:InExpr
        if context.select_stmt():
            # return SelectInExpr
            result =  None
        elif context.table_function_name():
            schema = process_schema_name(context.schema_name())
            table_func_name = process_any_name_context(context.table_function_name().any_name())
            params = [parse_expr(context )for context in context.expr()[1:]]
            result =  TableFuncNameInExpr(first_expr,schema,table_func_name,params)
        elif context.table_name():
            schema = process_schema_name(context.schema_name())
            table_name = process_table_name(context.table_name())
            result = TableNameInExpr(first_expr,schema,table_name)
        elif context.OPEN_PAR():
            in_exprs = [parse_expr(context )for context in context.expr()[1:]]
            result =  ExprsInExpr(first_expr,in_exprs)
        else:
            result =  SingleExprInExpr(first_expr,second_expr)
        result.negative = negative
        return result
    elif context.select_stmt():
        select:SelectStmt
        if context.NOT_():
            return SelectExpr(True,select)
        else:
            return SelectExpr(False,select)
    elif context.CASE_():
        return None
    return None

def process_schema_name(schema_name_context:SQLiteParser.Schema_nameContext):
    return process_any_name_context(schema_name_context.any_name())
def process_table_name(table_name_context:SQLiteParser.Table_nameContext):
    return process_any_name_context(table_name_context.any_name())
def process_column_name(column_name_context:SQLiteParser.Column_nameContext):
    return process_any_name_context(column_name_context.any_name())

def process_any_name_context(any_name:SQLiteParser.Any_nameContext ):
    if any_name.IDENTIFIER():
        return get_text(any_name.IDENTIFIER())
    if any_name.keyword():
        return get_text(any_name.keyword().getChild(0))
    if any_name.STRING_LITERAL():
        return get_text(any_name.STRING_LITERAL())
    if any_name.any_name():
        return process_any_name_context(any_name.any_name())


def parse_signed_number(signed : SQLiteParser.Signed_numberContext):
    if not signed:
        return None
    value = parse_numeric(signed.NUMERIC_LITERAL())
    if signed.MINUS():
        value = - value
    return value

def get_type_name(type_name_context:SQLiteParser.Type_nameContext)->TypeName:
    name_context_list:List[SQLiteParser.NameContext] = type_name_context.name()
    name_list = [process_any_name_context(l.any_name()) for l in name_context_list]
    name = str.join('',name_list)
    num_a = parse_signed_number(type_name_context.signed_number(0))
    num_b = parse_signed_number(type_name_context.signed_number(1))
    type_name = TypeName(name,num_a,num_b)
    return type_name


def get_text(terminal:TerminalNodeImpl)->str:
    if not terminal:
        return None
    if not isinstance(terminal,TerminalNodeImpl):
        raise TypeError("expected a TerminalNodeImpl")
    return terminal.symbol.text

def parse_numeric(numeric: SQLiteParser.NUMERIC_LITERAL):
    text = get_text(numeric)
    if '0x' in text:
        text = text.replace('0x', '')
        return int(text, 16)
    return float(text)

def parse_numeric_literal(numeric: SQLiteParser.NUMERIC_LITERAL)->NumericLiteral:
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

def parse_unary_expr(context: SQLiteParser.ExprContext)->Expr:
    operator = get_text(context.unary_operator())
    expr = parse_expr(context.expr(0))
    if '-' == operator:
        return UnaryExpr(UnaryOperator.MINUS,expr)
    elif '+' == operator:
        return UnaryExpr(UnaryOperator.PLUS,expr)
    elif '~' == operator:
        return UnaryExpr(UnaryOperator.TILDE,expr)
    else:
        return UnaryExpr(UnaryOperator.NOT,expr)


def parse_stmt_list(stmt_list:SQLiteParser.Sql_stmt_listContext)->List[Stmt]:
    pass

def parse_stmt(stmt_context:SQLiteParser.Sql_stmtContext)->Stmt:
    explain = stmt_context.EXPLAIN_() is not None
    query_plan = stmt_context.QUERY_() is not None
    if stmt_context.alter_table_stmt():
        return None

    pass

def parse_alter_table_stmt(context:SQLiteParser.Alter_table_stmtContext)->AlterTable:
    pass