from . import ValueGenerator


class IntegerValueGenerator(ValueGenerator):

    def create_generator(self, **kwargs):
        def gen():
            i = 1
            while True:
                yield i
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
