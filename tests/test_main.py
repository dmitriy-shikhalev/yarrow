from unittest.mock import Mock, call, patch

from pika import BasicProperties
import pytest
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
@patch('yarrow.main.read_operator_list', return_value=[
    'example.example.Sum',
    'example.example.Mul'
])
def test_import_operators(read_operator_list_mock, import_module_mock):
    main.import_operators()

    read_operator_list_mock.assert_called_once_with()

    import_module_mock.assert_has_calls([
        call('example.example'),
        call('example.example'),
    ])


@patch('yarrow.main.import_module', side_effect=[
    Mock(),
    Mock(),
    AttributeError,
])
@patch('yarrow.main.read_operator_list', return_value=[
    'example.example.Sum',
    'example.example.Mul',
    'example.example.NotExistedOperator',
])
def test_import_operators_error_not_exists(read_operator_list_mock, import_module_mock):
    with pytest.raises(AttributeError):
        main.import_operators()

    read_operator_list_mock.assert_called_once_with()

    import_module_mock.assert_has_calls([
        call('example.example'),
        call('example.example'),
    ])


@patch('yarrow.main.import_module', side_effect=[
    Mock(),
    Mock(),
    ModuleNotFoundError,
])
@patch('yarrow.main.read_operator_list', return_value=[
    'example.example.Sum',
    'example.example.Mul',
    'example.not_exist_module.NotExistedOperator',
])
def test_import_operators_error_module_not_exists(read_operator_list_mock, import_module_mock):
    with pytest.raises(ModuleNotFoundError):
        main.import_operators()

    read_operator_list_mock.assert_called_once_with()

    import_module_mock.assert_has_calls([
        call('example.example'),
        call('example.example'),
    ])


@patch('yarrow.main.import_module', return_value=Mock(
    String='abc',
))
@patch('yarrow.main.read_operator_list', return_value=[
    'example.example.Sum',
    'example.example.Mul',
    'example.example.String',
])
def test_import_operators_error_not_callable(read_operator_list_mock, import_module_mock):
    with pytest.raises(ValueError):
        main.import_operators()

    read_operator_list_mock.assert_called_once_with()

    assert import_module_mock.call_count == 3
    import_module_mock.assert_has_calls([
        call('example.example'),
        call('example.example'),
        call('example.example'),
    ])


@patch('yarrow.main.import_operators', return_value=[
    ('Sum', Mock()),
    ('Mul', Mock()),
])
@patch('yarrow.main.BlockingConnection')
@patch('yarrow.main.ConnectionParameters')
@patch('yarrow.main.PlainCredentials')
@patch('yarrow.main.Settings', return_value=Mock())
def test_serve(
        settings_mock,
        plain_credentials_mock,
        connection_parameters_mock,
        blocking_connection_mock,
        import_operators_mock
):
    main.serve()

    settings_mock.assert_called_once_with()

    plain_credentials_mock.assert_called_once_with(
        settings_mock.return_value.USERNAME,
        settings_mock.return_value.PASSWORD,
    )
    connection_parameters_mock.assert_called_once_with(
        host=settings_mock.return_value.HOST,
        port=settings_mock.return_value.PORT,
        virtual_host=settings_mock.return_value.VIRTUAL_HOST,
        credentials=plain_credentials_mock.return_value,
    )
    blocking_connection_mock.assert_called_once_with(
        parameters=connection_parameters_mock.return_value
    )

    channel = blocking_connection_mock.return_value.channel.return_value

    channel.queue_declare.assert_any_call('Sum')
    channel.queue_declare.assert_any_call('Mul')

    channel.basic_consume.assert_any_call('Sum', import_operators_mock.return_value[0][1])
    channel.basic_consume.assert_any_call('Mul', import_operators_mock.return_value[1][1])

    channel.start_consuming.assert_called_once_with()

    channel.close.assert_called_once_with()
    blocking_connection_mock.return_value.close.assert_called_once_with()


@patch('yarrow.main.import_operators')
def test_get_info(import_operators_mock, operator):
    import_operators_mock.return_value = [
        ('TestOperator', operator)
    ]

    channel = Mock()
    method_frame = Mock()
    properties = Mock()
    body = None

    main.get_info(channel, method_frame, properties, body)

    import_operators_mock.assert_called_once_with()

    channel.basic_publish.assert_called_once_with(
        exchange='',
        routing_key=properties.reply_to,
        properties=BasicProperties(correlation_id=properties.correlation_id),
        body=b'[{"name": "TestOperator", "input": {"properties": {"a": {"title": "A", "type": "integer"}}, "required": '
             b'["a"], "title": "TestModel", "type": "object"}, "output": {"properties": {"a": {"title": "A", '
             b'"type": "integer"}}, "required": ["a"], "title": "TestModel", "type": "object"}}]'
    )
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)


@patch('yarrow.main.import_operators')
def test_get_info_no_reply_to(import_operators_mock, operator):
    import_operators_mock.return_value = [
        ('TestOperator', operator)
    ]

    channel = Mock()
    method_frame = Mock()
    properties = Mock(reply_to=None)
    body = None

    main.get_info(channel, method_frame, properties, body)

    import_operators_mock.assert_not_called()

    channel.basic_publish.assert_not_called()
    channel.basic_ack.assert_called_once_with(method_frame.delivery_tag)
