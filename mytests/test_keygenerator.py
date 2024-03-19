import unittest
from mongo_memoize import PickleMD5KeyGenerator

class TestPickleMD5KeyGenerator(unittest.TestCase):

    def test_constructor_default_protocol(self):
        generator = PickleMD5KeyGenerator()
        self.assertEqual(generator._protocol, -1)

    def test_constructor_custom_protocol(self):
        generator = PickleMD5KeyGenerator(protocol=2)
        self.assertEqual(generator._protocol, 2)

    def test_call_with_different_args_order(self):
        generator = PickleMD5KeyGenerator()
        key1 = generator('my_function', (1, 2), {'a': 3, 'b': 4})
        # key2 = generator('my_function', (2, 1), {'b': 4, 'a': 3})
        key2 = generator('my_function', (1, 2), {'b': 4, 'a': 3})
        self.assertEqual(key1, key2)

    def test_call_with_different_pickle_protocols(self):
        generator1 = PickleMD5KeyGenerator(protocol=2)
        generator2 = PickleMD5KeyGenerator(protocol=3)
        result1 = generator1('my_function', (1, 2), {'a': 3})
        result2 = generator2('my_function', (1, 2), {'a': 3})
        self.assertNotEqual(result1, result2)

if __name__ == '__main__':
    unittest.main()
