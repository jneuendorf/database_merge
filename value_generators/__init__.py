from .ValueGenerator import ValueGenerator
from .StringValueGenerator import StringValueGenerator
from .IntegerValueGenerator import IntegerValueGenerator


def value_generator_for_type(cls: object, **kwargs) -> ValueGenerator:
    generator_by_type = {
        int: IntegerValueGenerator,
        str: StringValueGenerator,
    }
    return generator_by_type[cls](**kwargs).generator
