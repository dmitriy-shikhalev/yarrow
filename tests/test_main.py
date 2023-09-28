from unittest.mock import Mock, patch

from yarrow import main


@patch('yarrow.main.yaml.load', return_value=Mock())
@patch('yarrow.main.open', return_value=Mock(
    __enter__=Mock(return_value=open('example/config.yaml')),
    __exit__=Mock(),
))
@patch('yarrow.main.Settings', return_value=Mock())
def test_read_operator_list(settings_mock, open_mock, yaml_mock):
    operators = main.read_operator_list()

    assert operators == [
        'example.example.Sum',
        'example.example.Mul',
    ]

    settings_mock.assert_called_once_with()

    open_mock.assert_called_once_with(settings_mock.return_value.CONFIG_FILENAME, encoding='utf-8')
    open_mock.return_value.__enter__.assert_called_once_with()
    open_mock.return_value.__exit__.assert_called_once_with(None, None, None)
