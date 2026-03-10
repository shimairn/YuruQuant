from pathlib import Path
import unittest

from quantframe.app.config import load_config, load_resources


class ConfigAndResourcesTest(unittest.TestCase):
    def test_load_sample_config_and_resources(self):
        config = load_config(Path("resources/configs/gm_turtle_breakout.yaml"))
        resources = load_resources(config)
        self.assertEqual(config.platform.name, "gm")
        self.assertEqual(config.strategy.factory, "strategies.trend.turtle_breakout:create_strategy")
        self.assertEqual(len(resources.universe), 4)
        self.assertIn("DCE.P", resources.by_id)
        self.assertIn("SHFE.RB", resources.by_id)


if __name__ == "__main__":
    unittest.main()
