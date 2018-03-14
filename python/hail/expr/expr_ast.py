import hail
from hail.typecheck import *
from hail.utils.java import Env, escape_str, escape_id
from .expressions.indices import Indices
from typing import *
import abc

asttype = lazy()

class AST(object):
    @typecheck_method(children=asttype)
    def __init__(self, *children):
        self.typ = None
        self.children = children

    def to_hql(self):
        pass

    def to_ir(self):
        raise NotImplementedError()

    def expand(self):
        return self.search(lambda _: True)

    def search(self, f, l=None) -> Tuple['AST']:
        """
        Recursively searches the AST for nodes matching some pattern.
        """
        if l is None:
            l = []

        if (f(self)):
            l.append(self)
        for c in self.children:
            c.search(f, l)
        return tuple(l)


asttype.set(AST)


class _Reference(AST):
    __metaclass__ = abc.ABCMeta

    def __init__(self, name):
        self.name = name
        super(_Reference, self).__init__()

    def to_hql(self):
        return escape_id(self.name)

    def to_ir(self):
        return '(Ref {})'.format(escape_id(self.name))


class VariableReference(_Reference):
    @typecheck_method(name=str)
    def __init__(self, name: str):
        super(VariableReference, self).__init__(name)


class TopLevelReference(_Reference):
    def __init__(self, name: str, indices: Indices):
        self.indices = indices
        super(TopLevelReference, self).__init__(name)


class UnaryOperation(AST):
    @typecheck_method(parent=AST, operation=str)
    def __init__(self, parent, operation):
        self.parent = parent
        self.operation = operation
        super(UnaryOperation, self).__init__(parent)

    def to_hql(self):
        return '({}({}))'.format(self.operation, self.parent.to_hql())

    def to_ir(self):
        return '(ApplyUnaryPrimOp {} {})'.format(
            escape_id(self.operation),
            self.parent.to_ir())


class BinaryOperation(AST):
    @typecheck_method(left=AST, right=AST, operation=str)
    def __init__(self, left, right, operation):
        self.left = left
        self.right = right
        self.operation = operation
        super(BinaryOperation, self).__init__(left, right)

    def to_hql(self):
        return '({} {} {})'.format(self.left.to_hql(), self.operation, self.right.to_hql())

    def to_ir(self):
        return '(ApplyBinaryPrimOp {} {} {})'.format(
            escape_id(self.operation),
            self.left.to_ir(),
            self.right.to_ir())


class Select(AST):
    @typecheck_method(parent=AST, name=str)
    def __init__(self, parent, name):
        self.parent = parent
        self.name = name
        super(Select, self).__init__(parent)

        # TODO: create nested selection option

    def to_hql(self):
        return '{}.{}'.format(self.parent.to_hql(), escape_id(self.name))

    def to_ir(self):
        return '(GetField {} {})'.format(
            self.parent.to_ir(),
            escape_id(self.name))


class ApplyMethod(AST):
    @typecheck_method(method=str, args=AST)
    def __init__(self, method, *args):
        self.method = method
        self.args = args
        super(ApplyMethod, self).__init__(*args)

    def to_hql(self):
        return '{}({})'.format(self.method, ', '.join(ast.to_hql() for ast in self.args))


class ClassMethod(AST):
    @typecheck_method(method=str, callee=AST, args=AST)
    def __init__(self, method, callee, *args):
        self.method = method
        self.callee = callee
        self.args = args
        super(ClassMethod, self).__init__(callee, *args)

    def to_hql(self):
        return '{}.{}({})'.format(self.callee.to_hql(), self.method, ', '.join(ast.to_hql() for ast in self.args))


class LambdaClassMethod(AST):
    @typecheck_method(method=str, lambda_var=str, callee=AST, rhs=AST, args=AST)
    def __init__(self, method, lambda_var, callee, rhs, *args):
        self.method = method
        self.lambda_var = lambda_var
        self.callee = callee
        self.rhs = rhs
        self.args = args
        super(LambdaClassMethod, self).__init__(callee, rhs, *args)

    def to_hql(self):
        if self.args:
            return '{}.{}({} => {}, {})'.format(self.callee.to_hql(), self.method, self.lambda_var, self.rhs.to_hql(),
                                                ', '.join(a.to_hql() for a in self.args))
        else:
            return '{}.{}({} => {})'.format(self.callee.to_hql(), self.method, self.lambda_var, self.rhs.to_hql())


class Index(AST):
    @typecheck_method(parent=AST, key=AST)
    def __init__(self, parent, key):
        self.parent = parent
        self.key = key
        super(Index, self).__init__(parent, key)

    def to_hql(self):
        return '{}[{}]'.format(self.parent.to_hql(), self.key.to_hql())

    def to_ir(self):
        if isinstance(self.typ, hail.tarray):
            return '(ArrayRef {} {})'.format(
                self.parent.to_ir(),
                self.key.to_ir())
        elif isinstance(self.typ, hail.ttuple):
            assert isinstance(self.key, int)
            return '(GetTupleElement {} {})'.format(
                self.parent.to_ir(),
                self.key.to_ir())
        else:
            raise NotImplementedError()


class Literal(AST):
    @typecheck_method(value=str)
    def __init__(self, value):
        self.value = value
        super(Literal, self).__init__()

    def to_hql(self):
        return '({})'.format(self.value)

    def to_ir(self):
        print('value', self.value)
        if self.value is None:
            return '(NA [{}]'.format(
                self.typ._jtype.parsableString())
        elif self.typ == hail.tint32:
            return '(I32 {})'.format(int(self.value[4:]))
        elif self.typ == hail.tint64:
            assert isinstance(self.value, int)
            return '(I64 {})'.format(int(self.value[4:]))
        elif self.typ == hail.tfloat32:
            assert isinstance(self.value, float)
            return '(F32 {})'.format(float(self.value[4:]))
        elif self.typ == hail.tfloat64:
            assert isinstance(self.value, float)
            return '(F64 {})'.format(float(self.value[4:]))
        elif self.typ == hail.tbool:
            assert isinstance(self.value, bool)
            if self.value:
                return 'true'
            else:
                return 'false'
        elif self.typ == hail.tstr:
            assert isinstance(self.value, str)
            return '(StringLiteral {})'.format(self.value)
        else:
            raise NotImplementedError()


class ArrayDeclaration(AST):
    @typecheck_method(values=listof(AST))
    def __init__(self, values):
        self.values = values
        super(ArrayDeclaration, self).__init__(*values)

    def to_hql(self):
        return '[ {} ]'.format(', '.join(c.to_hql() for c in self.values))

    def to_ir(self):
        return '(MakeArray {})'.format(
            ' '.join(v.to_ir() for v in self.values))

class StructDeclaration(AST):
    @typecheck_method(keys=listof(str), values=listof(AST))
    def __init__(self, keys, values):
        self.keys = keys
        self.values = values
        super(StructDeclaration, self).__init__(*values)

    def to_hql(self):
        return '{' + ', '.join('{}: {}'.format(escape_id(k), v.to_hql()) for k, v in zip(self.keys, self.values)) + '}'

    def to_ir(self):
        return '(MakeStruct {})'.format(
            ' '.join('({} {})'.format(escape_id(k), v.to_ir())
                     for k, v in zip(self.keys, self.values)))


class TupleDeclaration(AST):
    @typecheck_method(values=AST)
    def __init__(self, *values):
        self.values = values
        super(TupleDeclaration, self).__init__(*values)

    def to_hql(self):
        return 'Tuple(' + ', '.join(v.to_hql() for v in self.values) + ')'

    def to_ir(self):
        return '(MakeTuple {})'.format(
            ' '.join(v.to_ir() for v in self.values))


class StructOp(AST):
    @typecheck_method(operation=str, parent=AST, keys=str)
    def __init__(self, operation, parent, *keys):
        self.operation = operation
        self.parent = parent
        self.keys = keys
        super(StructOp, self).__init__(parent)

    def to_hql(self):
        return '{}({}{})'.format(self.operation,
                                 self.parent.to_hql(),
                                 ''.join(', {}'.format(escape_id(x)) for x in self.keys))


class Condition(AST):
    @typecheck_method(predicate=AST, branch1=AST, branch2=AST)
    def __init__(self, predicate, branch1, branch2):
        self.predicate = predicate
        self.branch1 = branch1
        self.branch2 = branch2
        super(Condition, self).__init__(predicate, branch1, branch2)

    def to_hql(self):
        return '(if ({p}) {b1} else {b2})'.format(p=self.predicate.to_hql(),
                                                  b1=self.branch1.to_hql(),
                                                  b2=self.branch2.to_hql())

    def to_ir(self):
        return '(If {} {} {})'.format(
            self.predicate.to_ir(),
            self.branch1.to_ir(),
            self.branch2.to_ir())


class Slice(AST):
    @typecheck_method(start=nullable(AST), stop=nullable(AST))
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop
        super(Slice, self).__init__(*[x for x in [start, stop] if x is not None])

    def to_hql(self):
        return "{start}:{end}".format(start=self.start.to_hql() if self.start else '',
                                      end=self.stop.to_hql() if self.stop else '')


class Bind(AST):
    @typecheck_method(uid=str, definition=AST, expression=AST)
    def __init__(self, uid, definition, expression):
        self.uid = uid
        self.definition = definition
        self.expression = expression
        super(Bind, self).__init__(definition, expression)

    def to_hql(self):
        return "let {uid} = {left_expr} in {right_expr}".format(
            uid=self.uid,
            left_expr=self.definition.to_hql(),
            right_expr=self.expression.to_hql()
        )

    def to_ir(self):
        return '(Let {} {} {})'.format(
            escape_id(self.uid),
            self.left_expr.to_ir(),
            self.right_expr.to_ir())


class RegexMatch(AST):
    @typecheck_method(string=AST, regex=str)
    def __init__(self, string, regex):
        self.string = string
        self.regex = regex
        super(RegexMatch, self).__init__(string)

    def to_hql(self):
        return '("{regex}" ~ {string})'.format(regex=escape_str(self.regex), string=self.string.to_hql())


class AggregableReference(AST):
    def __init__(self):
        super(AggregableReference, self).__init__()

    def to_hql(self):
        return 'AGG'

    def to_ir(self):
        return 'AggIn'


class GlobalJoinReference(AST):
    def __init__(self, uid):
        self.uid = uid
        super(GlobalJoinReference, self).__init__()

    def to_hql(self):
        return 'global.{}'.format(escape_id(self.uid))

    def to_ir(self):
        return '(GetField (Ref global) {})'.format(
            escape_id(self.uid))
