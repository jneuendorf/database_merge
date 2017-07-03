from . import ValueGenerator


class StringValueGenerator(ValueGenerator):

    def __init__(self, prefix="", suffix="") -> None:
        super().__init__(
            prefix=prefix,
            suffix=suffix,
        )

    def create_generator(self, **kwargs):
        prefix = kwargs["prefix"]
        suffix = kwargs["suffix"]
        def gen():
            i = 1
            while True:
                yield f"{prefix}{str(i)}{suffix}"
                i += 1
        return gen()
