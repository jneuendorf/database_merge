import unittest

import value_generators


class ValueGeneratorTest(unittest.TestCase):

    ITER_MAX = 20


    ###########################################################################
    # TESTS
    def test_int_value_generator(self):
        int_vg = value_generators.value_generator_for_type(int)

        self.assertEqual(
            [next(int_vg) for i in range(0, self.ITER_MAX)],
            list(range(1, self.ITER_MAX + 1))
        )

    def test_str_value_generator(self):
        str_vg = value_generators.value_generator_for_type(str)
        self.assertEqual(
            [next(str_vg) for i in range(0, self.ITER_MAX)],
            [str(i) for i in range(1, self.ITER_MAX + 1)],
        )

        str_vg = value_generators.value_generator_for_type(
            str,
            prefix="prefix_",
            suffix="_suffix",
        )
        self.assertEqual(
            [next(str_vg) for i in range(0, self.ITER_MAX)],
            [f"prefix_{str(i)}_suffix" for i in range(1, self.ITER_MAX + 1)],
        )
