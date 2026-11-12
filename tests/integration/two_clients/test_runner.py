import unittest

from runner import metadata_process


class RunnerMetadataTest(unittest.TestCase):
    def test_process_snapshot_is_metadata_only(self):
        result = metadata_process({
            "process_instance_id": "p1",
            "pid": 10,
            "pgid": 10,
            "state": "READY",
            "executable": "/secret/path/llama-server",
            "arguments": ["--model", "/secret/model.gguf"],
        })
        self.assertEqual(result["process_instance_id"], "p1")
        self.assertNotIn("executable", result)
        self.assertNotIn("arguments", result)


if __name__ == "__main__":
    unittest.main()
