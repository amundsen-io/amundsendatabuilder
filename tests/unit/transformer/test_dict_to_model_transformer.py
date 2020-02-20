import unittest

from pyhocon import ConfigFactory

from databuilder.transformer.dict_to_model import DictToModel, MODEL_CLASS
from databuilder.models.dashboard.dashboard_last_execution import DashboardLastExecution


class TestDictToModel(unittest.TestCase):

    def test_conversion(self):
        # type: () -> None

        transformer = DictToModel()
        config = ConfigFactory.from_dict({
            MODEL_CLASS: 'databuilder.models.dashboard.dashboard_last_execution.DashboardLastExecution',
        })
        transformer.init(conf=config)

        actual = transformer.transform(
            {
                'dashboard_group_id': 'foo',
                'dashboard_id': 'bar',
                'execution_timestamp': 123456789,
                'execution_state': 'succeed',
                'product': 'mode',
                'cluster': 'gold'
            }
        )

        self.assertTrue(isinstance(actual, DashboardLastExecution))
        self.assertEqual(actual.__repr__(), DashboardLastExecution(
            dashboard_group_id='foo',
            dashboard_id='bar',
            execution_timestamp=123456789,
            execution_state='succeed',
            product='mode',
            cluster='gold'
        ).__repr__())


if __name__ == '__main__':
    unittest.main()
