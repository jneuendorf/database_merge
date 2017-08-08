import unittest

import strategies


class StrategyTest(unittest.TestCase):

    @classmethod
    def get_test_rows(cls):
        return [
            [
                (("source0",), "source"),
                (("target0",), "target"),
            ],
            [
                (("target1",), "target"),
                (("source1",), "source"),
            ],
            [
                (("source2.1",), "source"),
                (("source2.2",), "source"),
            ],
            [
                (("target3.1",), "target"),
                (("target3.2",), "target"),
            ],
        ]


    ###########################################################################
    # TESTS
    def test_merge_abstract(self):
        strategy = strategies.MergeStrategy()
        rows_datas = self.get_test_rows()
        def using_abstract_classes_instance():
            return strategy.choose_row(*rows_datas[0])
        self.assertRaises(NotImplementedError, using_abstract_classes_instance)

    def test_merge_source(self):
        strategy = strategies.SourceMergeStrategy()
        rows_datas = self.get_test_rows()

        self.assertEqual(strategy.choose_row(*rows_datas[0]), ["source0"])
        self.assertEqual(strategy.choose_row(*rows_datas[1]), ["source1"])
        self.assertEqual(strategy.choose_row(*rows_datas[2]), ["source2.1"])
        def raises():
            return strategy.choose_row(*rows_datas[3])
        self.assertRaises(ValueError, raises)

    def test_merge_target(self):
        strategy = strategies.TargetMergeStrategy()
        rows_datas = self.get_test_rows()

        self.assertEqual(strategy.choose_row(*rows_datas[0]), ["target0"])
        self.assertEqual(strategy.choose_row(*rows_datas[1]), ["target1"])
        def raises():
            return strategy.choose_row(*rows_datas[2])
        self.assertRaises(ValueError, raises)
        self.assertEqual(strategy.choose_row(*rows_datas[3]), ["target3.1"])
