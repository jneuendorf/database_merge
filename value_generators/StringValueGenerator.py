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
            i = 0
            while True:
                yield f"{prefix}{str(i)}{suffix}"
                i += 1
        return gen()


# >>> def gen():
# ...  i = 0
# ...  while (True):
# ...   yield i
# ...   i += 1
# ...
# >>> next(gen)
# Traceback (most recent call last):
#   File "<stdin>", line 1, in <module>
# TypeError: 'function' object is not an iterator
# >>> g = gen()
# >>> next(g)
# 0
# >>> next(g)
# 1
