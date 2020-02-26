import unittest

from pyhocon import ConfigFactory

from databuilder.transformer.template_variable_substitution_transformer import TEMPLATE, FIELD_NAME, \
    TemplateVariableSubstitutionTransformer


class TestTemplateVariableSubstitutionTransformer(unittest.TestCase):

    def test_new_field(self):
        # type: (...) -> None
        transformer = TemplateVariableSubstitutionTransformer()
        config = ConfigFactory.from_dict({
            TEMPLATE: '{foo}_{bar}',
            FIELD_NAME: 'new_field',
        })
        transformer.init(conf=config)

        original_record = {
            'foo': 'hello',
            'bar': 'world',
        }
        actual = transformer.transform(original_record)
        expected = {
            'foo': 'hello',
            'bar': 'world',
            'new_field': 'hello_world'
        }

        self.assertEqual(actual, expected)

    def test_update(self):
        # type: (...) -> None
        transformer = TemplateVariableSubstitutionTransformer()
        config = ConfigFactory.from_dict({
            TEMPLATE: '{foo}_{bar}',
            FIELD_NAME: 'bar',
        })
        transformer.init(conf=config)

        original_record = {
            'foo': 'hello',
            'bar': 'world',
        }
        actual = transformer.transform(original_record)
        expected = {
            'foo': 'hello',
            'bar': 'hello_world',
        }

        self.assertEqual(actual, expected)
