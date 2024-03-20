import unittest
from mongo_memoize import NoopSerializer, PickleSerializer

class TestSerializers(unittest.TestCase):

    def test_noop_serializer(self):
        serializer = NoopSerializer()
        obj = {"data": True}

        # Test serialization
        serialized = serializer.serialize(obj)
        self.assertEqual(serialized, obj)

        # Test deserialization
        deserialized = serializer.deserialize(serialized)
        self.assertEqual(deserialized, obj)

    def test_pickle_serializer(self):
        serializer = PickleSerializer()
        obj = {"data": 42, "list": [1, 2, 3]}

        # Test serialization
        serialized = serializer.serialize(obj)
        self.assertNotEqual(serialized, obj)  # Serialized data should be different
        self.assertIsInstance(serialized, bytes)  # Serialized data should be bytes

        # Test deserialization
        deserialized = serializer.deserialize(serialized)
        self.assertEqual(deserialized, obj)

    def test_pickle_serializer_with_protocol(self):
        serializer = PickleSerializer(protocol=4)
        obj = {"data": "string", "nested": {"value": 10}}

        # Test serialization with specified protocol
        serialized = serializer.serialize(obj)
        deserialized = serializer.deserialize(serialized)
        self.assertEqual(deserialized, obj)

if __name__ == '__main__':
    unittest.main()
