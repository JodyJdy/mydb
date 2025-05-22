from enum import Enum
from typing import List, Tuple


class ConflictClause(Enum):
    ROLLBACK = 1
    ABORT = 2
    FAIL = 3
    IGNORE = 4
    REPLACE = 5

class Stmt:
    def __init__(self):
        self.explain:bool = False
        self.query_plan:bool = False

class Expr:
    pass

class LiteralValue(Expr):
    pass
class TrueLiteral(Expr):
    pass
class FalseLiteral(Expr):
    pass
class NumericLiteral(Expr):
    def __init__(self, value:float):
        self.value = value
class StringLiteral(Expr):
    def __init__(self, value:str):
        self.value = value
class NullLiteral(Expr):
    pass
class CurrentTime(Expr):
    pass
class CurrentDate(Expr):
    pass
class CurrentTimeStamp(Expr):
    pass



class StmtList(Stmt):
    def __init__(self, stmt_list: List[Stmt]):
        super().__init__()
        self.stmt_list = stmt_list

    def __iter__(self):
        if not self.stmt_list or self.stmt_list == []:
            return iter([])
        return iter(self.stmt_list)


class CommonTable:
    def __init__(self):
        self.table_name:str = None
        self.columns:List[str] = None
        self.select_stmt:SelectStmt = None
class SelectStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.recursive:bool = None
        self.common_table_list:List[CommonTable] = []
        self.select_core:SelectCore = None
        self.order_by:List[OrderingTerm] = None
        self.limit:Limit = None

class QueryTable:
    pass
class SimpleQueryTable(QueryTable):
    def __init__(self,schema_name:str,table_name:str,table_alias:str,index_name:str):
        self.schema_name:str = schema_name
        self.table_name:str = table_name
        self.table_alias:str = table_alias
        """指定查询时用的索引"""
        self.indexed_by:str = index_name
class TableFuncQueryTable(QueryTable):
    def __init__(self,schema_name:str,table_func_name:str,params:List[Expr],table_alias:str):
        """返回一个table的函数"""
        self.schema_name:str = schema_name
        self.table_func_name:str = table_func_name
        self.params:List[Expr] = params
        self.table_alias:str = table_alias
class SelectStmtQueryTable(QueryTable):
    def __init__(self,select_stmt:SelectStmt,table_alias:str):
        self.select_stmt:SelectStmt = select_stmt
        self.table_alias:str = table_alias

class JoinTypeEnum(Enum):
    #笛卡尔
    CARTESIAN = 1
    LEFT = 2
    RIGHT = 3
    FULL = 4
    INNER = 5
    CROSS = 6

class JoinConstraint:
    """连接条件"""
    pass
class UsingJoinConstraint(JoinConstraint):
    """使用同名字段连接"""
    def __init__(self,column_name:List[str]):
        self.column_name:List[str] = column_name
class OnJoinConstraint(JoinConstraint):
    """使用 on 连接"""
    def __init__(self,expr:Expr):
        self.expr:Expr = expr
class JoinQueryTable(QueryTable):
    """连表"""
    def __init__(self,left:QueryTable,right:QueryTable,natural=None,join_type=None,join_constraint = None):
        self.left:QueryTable = left
        self.right:QueryTable = right
        self.natural:bool = natural
        self.join_type:JoinTypeEnum = join_type
        self.join_constraint:JoinConstraint = join_constraint


class SelectCore:
    pass

class WindowFunc:
    pass

class NormalSelectCore(SelectCore):
    def __init__(self):
        self.distinct:bool=None
        self.result_columns:List[ReturningClause] = None
        self.query_table:QueryTable = None
        self.where:Expr = None
        self.group_by:List[Expr] = None
        self.having:Expr = None

class ValuesClauseSelectCore(SelectCore):
    def __init__(self,rows:List[List[Expr]]):
        self.rows:List[List[Expr]] = rows


class UnionSelectCore(SelectCore):
    def __init__(self,all_:bool,left:SelectCore,right:SelectCore):
        self.all_:bool = all_
        self.left:SelectCore = left
        self.right:SelectCore = right
class IntersectSelectCore(SelectCore):
    def __init__(self):
        self.left:SelectCore = None
        self.right:SelectCore = None
class ExceptSelectCore(SelectCore):
    def __init__(self):
        self.left:SelectCore = None
        self.right:SelectCore = None

class InsertTypeEnum(Enum):
    INSERT = 1
    REPLACE = 2
    INSERT_OR_REPLACE = 3
    INSERT_OR_ROLLBACK = 4
    INSERT_OR_ABORT = 5
    INSERT_OR_FAIL = 6
    INSERT_OR_IGNORE = 7

class UpdateTypeEnum(Enum):
    UPDATE = 1
    UPDATE_OR_ROLLBACK = 2
    UPDATE_OR_ABORT = 3
    UPDATE_OR_REPLACE = 4
    UPDATE_OR_FAIL = 5
    UPDATE_OR_IGNORE = 6

class UpdateSet:
    def __init__(self,expr:Expr,column_name:str=None,columns:List[str]=None):
        self.column_name:str = column_name
        self.columns:List[str] = columns
        self.expr:Expr = expr

class UpdateStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.with_clause:WithClause = None
        self.qualified_name:QualifiedTableName = None
        self.update_type:UpdateTypeEnum = None
        self.update_set:List[UpdateSet] = None
        self.from_ : QueryTable = None
        self.where:Expr = None
        self.return_clauses:List[ReturningClause] = None
        self.order_by:List[OrderingTerm] = None
        self.limit:Limit = None

class UpsertClause:
    pass
class InsertStmt(Stmt):
    def __init__(self):
        self.insert_type:InsertTypeEnum = None
        self.schema_name:str|None = None
        self.table_name:str = None
        self.table_alias:str|None = None
        self.column_names:List[str] = None
        self.values_clause:List[List[Expr]] = None
        self.select_stmt:SelectStmt = None
        self.return_clauses:List[ReturningClause] = None

class DeleteStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.limit:Limit|None = None
        self.order_by:None|List[OrderingTerm] = None
        self.returning_clauses:List[ReturningClause] = None
        self.where: Expr | None = None
        self.qualified_table_name:QualifiedTableName = None
        self.with_clause:WithClause|None = None

class ColumnConstraint:
    def __init__(self,name:str):
        self.name:str = name
class TableConstraint:
    def __init__(self,name:str):
        self.name:str = name
    pass
class PrimaryKeyConstraint(ColumnConstraint):
    def __init__(self,name:str,isAsc:bool,conflict_clause:ConflictClause,autoincrement:bool):
        super().__init__(name)
        self.isAsc:bool = isAsc
        self.conflict_clause:ConflictClause = conflict_clause
        self.autoincrement:bool = autoincrement
class NullConstraint(ColumnConstraint):
    def __init__(self,name:str,negative:bool):
        super().__init__(name)
        self.negative:bool=negative
class UniqueConstraint(ColumnConstraint):
    def __init__(self,name:str):
        super().__init__(name)
class CheckConstraint(ColumnConstraint):
    def __init__(self,name:str,expr:Expr):
        super().__init__(name)
        self.expr:Expr = expr
class DefaultConstraint(ColumnConstraint):
    def __init__(self,name:str,expr:Expr):
        super().__init__(name)
        self.expr:Expr = expr
class CollateConstraint(ColumnConstraint):
    def __init__(self,name:str,collation_name:str):
        super().__init__(name)
        self.collation_name:str = collation_name
class AsConstraint(ColumnConstraint):
    def __init__(self,name:str,generated_always:bool,expr:Expr,stored:bool):
        super().__init__(name)
        self.generated_always:bool = generated_always
        self.expr:Expr = expr
        # stored or virtual
        self.stored:bool = stored


class ForeignKeyActionType(Enum):
    SET_NULL = 1
    SET_DEFAULT =2
    CASCADE = 3
    RESTRICT = 4
    NO_ACTION =5

    @staticmethod
    def parse_action(action:str):
        if "NULL" == action:
            return ForeignKeyActionType.SET_NULL
        elif "DEFAULT" == action:
            return ForeignKeyActionType.SET_DEFAULT
        elif "CASCADE" == action:
            return ForeignKeyActionType.CASCADE
        elif "RESTRICT" == action:
            return ForeignKeyActionType.RESTRICT
        elif "NO" == action:
            return ForeignKeyActionType.NO_ACTION

class ForeignKeyOnClause:
    def __init__(self,is_delete:bool,action_type:ForeignKeyActionType):
        # delete还是update
        self.is_delete:bool = is_delete
        self.action_type = action_type

class ForeignKeyClause(ColumnConstraint):
    def __init__(self,name:str|None,foreign_table:str,column_names:List[str],on_clauses:List[ForeignKeyOnClause],immediate:bool):
        super().__init__(name)
        self.on_clauses:List[ForeignKeyOnClause] = on_clauses
        """
        DEFERRABLE INITIALLY DEFERRED -- A deferred foreign key constraint
        NOT DEFERRABLE INITIALLY DEFERRED            -- An immediate foreign key constraint
        NOT DEFERRABLE INITIALLY IMMEDIATE           -- An immediate foreign key constraint
        NOT DEFERRABLE                               -- An immediate foreign key constraint
        DEFERRABLE INITIALLY IMMEDIATE               -- An immediate foreign key constraint
        DEFERRABLE                                   -- An immediate foreign key constraint
        """
        self.immediate:bool = immediate
        self.column_names:List[str] = column_names
        self.foreign_table:str = foreign_table

class TypeName:
    def __init__(self,name:str,type_name_l:float,type_name_r:float):
        #decimal(10,5) 类型如果有范围需要记录
        self.name = name
        self.type_name_l:float = type_name_l
        self.type_name_r:float = type_name_r

class ColumnDef:
    def __init__(self,column_name:str,type_name:TypeName,column_constraint:List[ColumnConstraint]):
        self.column_name = column_name
        self.column_constraint:List[ColumnConstraint] = column_constraint
        #decimal(10,5) 类型如果有范围需要记录
        self.type_name:TypeName = type_name

class IndexedColumn:
    def __init__(self,collation_name:str,is_asc:bool) -> None:
        self.is_asc = is_asc
        self.collation_name = collation_name
class PrimaryTableConstraint(TableConstraint):
    def __init__(self,name:str,indexed_columns:List[IndexedColumn]):
        super().__init__(name)
        self.indexed_columns:List[IndexedColumn] = indexed_columns
class UniqueTableConstraint(TableConstraint):
    def __init__(self,name:str,indexed_columns:List[IndexedColumn]):
        super().__init__(name)
        self.indexed_columns:List[IndexedColumn] = indexed_columns
class CheckTableConstraint(TableConstraint):
    def __init__(self,name:str,expr:Expr):
        super().__init__(name)
        self.expr:Expr = expr
class ForeignKeyOnTableConstraint(TableConstraint):
    def __init__(self,name:str,column_names:List[str],foreign_clause:ForeignKeyClause):
        super().__init__(name)
        self.column_names:List[str] = column_names
        self.foreign_clause:ForeignKeyClause = foreign_clause


class AlterTable(Stmt):
    def __init__(self,schema_name:str,table_name:str):
        super().__init__()
        self.schema_name:str = schema_name
        self.table_name:str = table_name
class AlterTableRenameTable(AlterTable):
    def __init__(self,schema_name:str,table_name:str,new_table_name:str) -> None:
        super().__init__(schema_name,table_name)
        self.new_table_name:str = new_table_name
class AlterTableRenameColumn(AlterTable):
    def __init__(self,schema_name:str,table_name:str,old_column_name:str,new_column_name:str) -> None:
        super().__init__(schema_name,table_name)
        self.old_column_name:str = old_column_name
        self.new_column_name:str = new_column_name
class AlterTableAddColumn(AlterTable):
    def __init__(self,schema_name:str,table_name:str,column_def:ColumnDef) -> None:
        super().__init__(schema_name,table_name)
        self.column_def:ColumnDef = column_def
class AlterTableDropColumn(AlterTable):
    def __init__(self,schema_name:str,table_name:str,column_name:str) -> None:
        super().__init__(schema_name,table_name)
        self.column_name:str = column_name

class AnalyzeStmt(Stmt):
    def __init__(self,schema_name:str,table_or_index_name:str):
        super().__init__()
        self.schema_name = None
        self.table_or_index_name = None

    def schema_name(self, schema_name: str) -> None:
        self.schema_name = schema_name

    def table_or_index_name(self, table_or_index_name: str):
        self.table_or_index_name = table_or_index_name


class AttachStmt(Stmt):
    def __init__(self,data_base:bool,expr:Expr,schema_name:str):
        super().__init__()
        self.expr = expr
        self.schema_name = schema_name
        self.data_base:bool = data_base

class TransactionBeginType(Enum):
    DEFERRED = 1
    IMMEDIATE = 2
    EXCLUSIVE = 3



class BeginStmt(Stmt):
    def __init__(self,begin_type:TransactionBeginType,transaction_name:str):
        super().__init__()
        self.begin_type:TransactionBeginType = begin_type
        self.transaction_name = transaction_name

class CommitStmt(Stmt):
    pass


class ColumnIndexedColumn(IndexedColumn):
    def __init__(self,column_name:str,collation_name:str,is_asc:bool) -> None:
        super().__init__(collation_name,is_asc)
        self.column_name:str = column_name

class ExprIndexedColumn(IndexedColumn):
    def __init__(self,expr:Expr,collation_name:str,is_asc:bool) -> None:
        super().__init__(collation_name,is_asc)
        self.expr:Expr = expr

class CreateIndexStmt(Stmt):
    def __init__(self,unique:bool,if_not_exist:bool,schema_name:str,index_name:str,table_name:str,columns:List[ColumnIndexedColumn],where:Expr):
        super().__init__()
        self.unique:bool = unique
        self.if_not_exist:bool = if_not_exist
        self.schema_name = schema_name
        self.index_name = index_name
        self.table_name = table_name
        self.indexed_columns:List[IndexedColumn] = columns
        self.where:Expr = where


class CreateTableStmt(Stmt):
    def __init__(self,temp:bool,if_not_exist:bool,schema_name:str,table_name:str):
        super().__init__()
        #临时表
        self.temp:bool = temp
        self.table_name:str = table_name
        self.schema_name:str = schema_name
        self.if_not_exist:bool = if_not_exist

class UsuallyCreateTableStmt(CreateTableStmt):
    def __init__(self,temp:bool,if_not_exist:bool,schema_name:str,table_name:str,table_constraints:List[TableConstraint],column_defs:List[ColumnDef] ):
        super().__init__(temp,if_not_exist,schema_name,table_name)
        self.column_defs = column_defs
        self.table_constraints = table_constraints

class SelectCreateTableStmt(CreateTableStmt):
    def __init__(self,temp:bool,if_not_exist:bool,schema_name:str,table_name:str,select_stmt:SelectStmt):
        super().__init__(temp,if_not_exist,schema_name,table_name)
        self.select_stmt = select_stmt
class TriggerTimeEnum(Enum):
    """触发器作用的时机"""
    BEFORE =1
    AFTER =2
    INSTEAD_OF = 3
class TriggerStmtTypeEnum(Enum):
    """触发器作用stmt类型"""
    DELETE = 1
    INSERT = 2
    UPDATE =3

class CreateTriggerStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.temp:bool= None
        self.if_not_exist:bool = None
        self.schema_name:str = None
        self.trigger_name:str = None
        self.trigger_time:TriggerTimeEnum = None
        self.trigger_type:TriggerStmtTypeEnum = None
        """如果是更新，触发的列的名称"""
        self.update_column_names:List[str] = None
        self.table_name = None
        self.for_each_row:bool = None
        self.when:Expr = None
        self.stmt_list:List[Stmt] = None


class CreateViewStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.temp:bool = None
        self.if_not_exist:bool = None
        self.schema_name:str = None
        self.view_name:str = None
        self.column_names:List[str] = None
        self.select_stmt:SelectStmt = None


class ModuleArgument:
    pass
class ColumnDefModuleArgument(ModuleArgument):
    def __init__(self):
        super().__init__()
        self.column_def:ColumnDef = None
class ExpressionModuleArgument(ModuleArgument):
    def __init__(self):
        super().__init__()
        self.expr:Expr = None


class VirtualTableStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.if_not_exist:bool = None
        self.schema_name:str = None
        self.table_name:str = None
        self.module_name:str = None
        self.module_arguments:List[ModuleArgument] = None


class CteTableName:
    def __init__(self):
        self.column_names:List[str] = None
        self.table_name:str = None


class QualifiedTableName:
    def __init__(self):
        self.schema_name:str = None
        self.table_name:str = None
        self.alias_name:str = None
        self.index_name:str = None


class ReturningClause:
    def __init__(self):
        self.column_alias = None
        self.expr:Expr = None
        self.table_name:str = None
        self.star:bool = None


class OrderingTerm:
    def __init__(self):
        self.expr = None
        self.collation_name = None
        self.is_asc:bool = None
        self.null_first:bool = None


class WithClauseContent:
    def __init__(self,cte_table_name:CteTableName,select_stmt:SelectStmt):
        self.select_stmt:SelectStmt = select_stmt
        self.cte_table_name:CteTableName = cte_table_name


class WithClause:
    def __init__(self):
        self.clauses:List[WithClauseContent] = None
        self.recursive:bool = None


class Limit(Stmt):
    def __init__(self,limit:Expr,offset:Expr):
        super().__init__()
        self.limit:Expr = limit
        self.offset:Expr = offset



class DetachStmt(Stmt):
    def __init__(self,schema_name:str,database:bool):
        super().__init__()
        self.schema_name = schema_name
        self.database:bool = database

class ObjectTypeEnum(Enum):
    INDEX = 1
    TABLE = 2
    TRIGGER = 3
    VIEW = 4

class DropStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.name:str = None
        self.schema_name:str = None
        self.object_type:ObjectTypeEnum = None
        self.if_exist:bool = None


class PragmaValueType(Enum):
    NAME = 1
    STRING_LITERAL = 2
    NUMBER = 3

class PragmaValue:
    def __init__(self):
        self.value = None
        self.type:PragmaValueType = None

class PragmaStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.schema_name = None
        self.pragma_name = None
        self.pragma_value:PragmaValue = None
        self.assign:bool = None


class ReindexStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.table_or_index_name:str = None
        self.collation_name:str = None
        self.schema_name:str = None


class ReleaseStmt(Stmt):
    def __init__(self,save_point_name:str|None):
        super().__init__()
        self.save_point_name:str|None = save_point_name



class RollbackStmt(Stmt):
    def __init__(self,save_point_name:str|None):
        super().__init__()
        self.save_point_name:str|None = save_point_name

class SavePointStmt(Stmt):
    def __init__(self,save_point_name:str):
        super().__init__()
        self.save_point_name = save_point_name


class VacuumStmt(Stmt):
    def __init__(self):
        super().__init__()
        self.filename = None
        self.schema_name = None


class BindParameter(Expr):
    def __init__(self,digit,parameter):
        """两种形式 ?开头或者 :@$开头"""
        self.digit:bool = digit
        self.parameter:str = parameter

class ColumnName(Expr):
    def __init__(self,schema_name:str=None,table_name:str=None,column_name:str=None):
        self.schema_name:str = schema_name
        self.table_name:str = table_name
        self.column_name:str = column_name
class UnaryOperator(Enum):
    MINUS = 1
    PLUS = 2
    TILDE = 3
    NOT = 4
class UnaryExpr(Expr):
    def __init__(self,operator:UnaryOperator,expr:Expr):
        self.operator:UnaryOperator = operator
        self.expression:Expr = expr

class BinaryOperator(Enum):
    PIPE2 = 1
    STAR = 2
    DIV = 3
    MOD = 4
    PLUS = 5
    MINUS = 6
    LT2 = 7
    GT2 = 8
    AMP = 9
    PIPE = 10
    LT = 11
    LT_EQ =12
    GT =13
    GT_EQ =14
    ASSIGN = 15
    EQ = 16
    NOT_EQ1 =17
    NOT_EQ2 =18
    IS=19
    IS_NOT = 20
    DISTINCT_FROM = 21
    NOT_DISTINCT_FROM = 22
    IN = 23
    LIKE =24
    GLOB =25
    MATCH =26
    REGEX =27
    AND = 28
    OR = 29


class BinaryExpr(Expr):
    def __init__(self,operator:BinaryOperator,left:Expr,right:Expr):
        self.operator:BinaryOperator = operator
        self.left:Expr = left
        self.right:Expr = right

class FuncCall(Expr):
    """不考虑 filter, over"""
    def __init__(self,func_name:str,distinct:bool,star:bool,params:List[Expr]):
        self.func_name:str = func_name
        self.distinct:bool = distinct
        self.star = star
        self.params:List[Expr] = params
class Cast(Expr):
    def __init__(self,expr:Expr,type_name:TypeName):
        self.expr:Expr = expr
        self.type_name:TypeName = type_name
class ExprWithCollate(Expr):
    def __init__(self,expr:Expr,collate:str):
        self.expr:Expr = expr
        self.collate:str = collate

class TextMatchType(Enum):
    LIKE = 1
    GLOB = 2
    REGEX = 3
    MATCH = 4

class TextMatchExpr(Expr):
    def __init__(self,negative:bool,type:TextMatchType,left:Expr,right:Expr,escape:Expr):
        self.type:TextMatchType = type
        self.negative:bool = negative
        self.left:Expr = left
        self.right:Expr = right
        self.escape:Expr = escape
class IsExpr(Expr):
    def __init__(self,negative:bool,left:Expr,right:Expr,distinct=False):
        self.left:Expr = left
        self.right:Expr = right
        self.negative:bool = negative
        self.distinct:bool = distinct
class IsNullExpr(Expr):
    def __init__(self,expr:Expr,negative:bool):
        self.expr:Expr = expr
        self.negative:bool = negative
class BetweenExpr(Expr):
    def __init__(self,left:Expr,right:Expr,negative:bool=False):
        self.left:Expr = left
        self.right:Expr = right
        self.negative:bool = negative
class InExpr(Expr):
    def __init__(self,expr:Expr):
        self.negative:bool = None
        self.expr:Expr = expr
class SingleExprInExpr(InExpr):
    def __init__(self,expr:Expr,in_expr:Expr):
        super().__init__(expr)
        self.in_expr:Expr = in_expr
class SelectInExpr(InExpr):
    def __init__(self,expr:Expr,select_stmt:SelectStmt):
        super().__init__(expr)
        self.select_stmt:SelectStmt = select_stmt
class ExprsInExpr(InExpr):
    def __init__(self,expr:Expr,in_expr_list:List[Expr]):
        super().__init__(expr)
        self.in_expr_list:List[Expr] = in_expr_list
class TableNameInExpr(InExpr):
    def __init__(self,expr:Expr,schema_name:str,table_name:str):
        super().__init__(expr)
        self.schema_name:str = schema_name
        self.table_name:str = table_name
class TableFuncNameInExpr(InExpr):
    def __init__(self,expr:Expr,schema_name:str,table_func_name:str,params:List[Expr]):
        super().__init__(expr)
        self.schema_name:str = schema_name
        self.table_func_name:str = table_func_name
        self.params:List[Expr] = params
class CaseThenExpr(Expr):
    def __init__(self,case:Expr,when_then:List[Tuple[Expr,Expr]],else_expr:Expr):
        self.case:Expr = case
        self.when_then:List[(Expr,Expr)] = when_then
        self.else_expr:Expr = else_expr
class SelectExpr(Expr):
    def __init__(self,negative:bool,select_stmt:SelectStmt):
        self.negative:bool = negative
        self.select_stmt = select_stmt
#暂时不考虑
class Raise:
    pass
