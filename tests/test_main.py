from unittest.mock import Mock, patch

import yaml

from yarrow import main


@patch('yarrow.main.yaml.load_all', return_value=[
    {
        'operators': [
            'example.example.Sum',
            'example.example.Mul'
        ]
    }
])
@patch('yarrow.main.open', return_value=Mock(
    __enter__=Mock(return_value=open('example/config.yaml')),
    __exit__=Mock(),
))
@patch('yarrow.main.Settings', return_value=Mock())
def test_read_operator_list(settings_mock, open_mock, load_mock):
    operators = main.read_operator_list()

    assert operators == [
        'example.example.Sum',
        'example.example.Mul',
    ]

    settings_mock.assert_called_once_with()

    open_mock.assert_called_once_with(settings_mock.return_value.CONFIG_FILENAME, encoding='utf-8')
    open_mock.return_value.__enter__.assert_called_once_with()
    open_mock.return_value.__exit__.assert_called_once_with(None, None, None)

    load_mock.assert_called_once_with(open_mock.return_value.__enter__.return_value, Loader=yaml.Loader)


@patch('yarrow.main.import_module')
@patch('yarrow.main.pika')
@patch('yarrow.main.read_operator_list', return_value=[
    'example.example.Sum',
    'example.example.Mul'
])
@patch('yarrow.main.Settings', return_value=Mock())
def test_serve(settings_mock, read_operator_list_mock, pika_mock, import_module_mock):
    main.serve()

    settings_mock.assert_called_once_with()

    read_operator_list_mock.assert_called_once_with()

    pika_mock.PlainCredentials.assert_called_once_with(
        settings_mock.return_value.USERNAME,
        settings_mock.return_value.PASSWORD,
    )
    pika_mock.ConnectionParameters.assert_called_once_with(
        host=settings_mock.return_value.HOST,
        port=settings_mock.return_value.PORT,
        virtual_host=settings_mock.return_value.VIRTUAL_HOST,
        credentials=pika_mock.PlainCredentials.return_value,
    )
    pika_mock.BlockingConnection.assert_called_once_with(
        parameters=pika_mock.ConnectionParameters.return_value
    )

    channel = pika_mock.BlockingConnection.return_value.channel.return_value

    channel.queue_declare.assert_any_call('Sum')
    channel.queue_declare.assert_any_call('Mul')

    import_module_mock.assert_any_call('Sum', 'example.example')
    import_module_mock.assert_any_call('Mul', 'example.example')

    channel.basic_consume.assert_any_call('Sum', import_module_mock.return_value.Sum)
    channel.basic_consume.assert_any_call('Mul', import_module_mock.return_value.Mul)

    channel.start_consuming.assert_called_once_with()

    channel.close.assert_called_once_with()
    pika_mock.BlockingConnection.return_value.close.assert_called_once_with()
