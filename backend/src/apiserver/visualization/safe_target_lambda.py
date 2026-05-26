# Copyright 2026 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import ast
from typing import Callable
from typing import Text


_ALLOWED_NODE_TYPES = (
    ast.Expression,
    ast.Lambda,
    ast.arguments,
    ast.arg,
    ast.Load,
    ast.Name,
    ast.Constant,
    ast.Subscript,
    ast.BoolOp,
    ast.BinOp,
    ast.UnaryOp,
    ast.Compare,
    ast.IfExp,
    ast.List,
    ast.Tuple,
    ast.And,
    ast.Or,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.FloorDiv,
    ast.Mod,
    ast.UAdd,
    ast.USub,
    ast.Not,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
)


class _TargetLambdaValidator(ast.NodeVisitor):
    """Allows simple row expressions while rejecting executable Python syntax."""

    def __init__(self) -> None:
        self._lambda_argument = ''

    def visit(self, node: ast.AST) -> None:
        if not isinstance(node, _ALLOWED_NODE_TYPES):
            raise ValueError(
                'target_lambda contains unsupported expression: {}'.format(
                    type(node).__name__))
        super().visit(node)

    def visit_Expression(self, node: ast.Expression) -> None:
        if not isinstance(node.body, ast.Lambda):
            raise ValueError('target_lambda must be a lambda expression')
        self.visit(node.body)

    def visit_Lambda(self, node: ast.Lambda) -> None:
        args = node.args
        if (len(args.args) != 1 or args.posonlyargs or args.vararg or
                args.kwonlyargs or args.kw_defaults or args.kwarg or
                args.defaults):
            raise ValueError(
                'target_lambda must accept exactly one row argument')
        self._lambda_argument = args.args[0].arg
        self.visit(args)
        self.visit(node.body)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id != self._lambda_argument:
            raise ValueError(
                'target_lambda may only reference the row argument')


def build_safe_target_lambda(expression: Text) -> Callable[[object], object]:
    """Compiles a constrained lambda expression for ROC target mapping.

    The ROC visualization historically accepted a Python lambda string. Keep
    support for arithmetic and boolean expressions over the row object, but do
    not allow calls, imports, attributes, comprehensions, or other syntax that
    can execute attacker-controlled code in the visualization server process.
    """
    parsed_expression = ast.parse(expression, mode='eval')
    _TargetLambdaValidator().visit(parsed_expression)
    compiled_expression = compile(
        parsed_expression, '<target_lambda>', 'eval')
    target_lambda = eval(compiled_expression, {'__builtins__': {}}, {})
    if not callable(target_lambda):
        raise ValueError('target_lambda must compile to a callable')
    return target_lambda
