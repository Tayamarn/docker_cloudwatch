import argparse
import datetime
import logging
from unittest.mock import MagicMock
from unittest.mock import patch

import docker
import pytest

import main
import patched_cloudwatch


@pytest.fixture
def time_fixture():
    with patch("time.time") as mock_time:
        mock_time.return_value = 1719484518
        yield mock_time


@pytest.mark.parametrize("container_logs,expected_caplog,expected_aws", [
    # Happy path
    (
        [b'1', b'2', b'3'],
        [
            ('docker_cloudwatch', logging.INFO, '1'),
            ('docker_cloudwatch', logging.INFO, '2'),
            ('docker_cloudwatch', logging.INFO, '3'),
        ],
        [
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'logEvents': [{'timestamp': 1719484518000, 'message': '2024-06-27 13:35:18,000 : INFO - 1'}]},
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'sequenceToken': 1, 'logEvents': [{'timestamp': 1719484518000, 'message': '2024-06-27 13:35:18,000 : INFO - 2'}]},
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'sequenceToken': 1, 'logEvents': [{'timestamp': 1719484518000, 'message': '2024-06-27 13:35:18,000 : INFO - 3'}]}
        ],
    ),
    # Long message
    (
        [b'10000000000000000000000000000000000000000'],
        [
            ('docker_cloudwatch', logging.INFO, '10000000000000000000000000000000000000000'),
        ],
        [
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'logEvents': [{'timestamp': 1719484518000, 'message': '2024-06-27 13:35:18,000 : INFO - 1000000'}]},
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'sequenceToken': 1, 'logEvents': [{'timestamp': 1719484518000, 'message': '0000000000000000000000000000000000'}]},
        ],
    ),
    # Unicode
    (
        ['Альфа бета гамма дельта'.encode('utf-8')],
        [
            ('docker_cloudwatch', logging.INFO, 'Альфа бета гамма дельта'),
        ],
        [
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'logEvents': [{'timestamp': 1719484518000, 'message': '2024-06-27 13:35:18,000 : INFO - Аль'}]},
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'sequenceToken': 1, 'logEvents': [{'timestamp': 1719484518000, 'message': 'фа бета гамма дельта'}]},
        ],
    ),
])
def test_docker_cloudwatch(
        caplog,
        monkeypatch,
        time_fixture,
        container_logs,
        expected_caplog,
        expected_aws,
):
    logs = []
    token = 0

    class MockContainer:
        def logs(self, stream):
            return container_logs

    class MockDockerClient:
        @property
        def images(self):
            return MagicMock(get=MagicMock(return_value='image_name'))

        @property
        def containers(self):
            return MagicMock(run=MagicMock(return_value=MockContainer()))

    def mock_log_event(**kwargs):
        nonlocal logs
        nonlocal token
        logs.append(kwargs)
        return {'nextSequenceToken': 1}

    class MockSession:
        def client(self, *args):
            return MagicMock(
                put_log_events=MagicMock(side_effect=mock_log_event),
            )

    def mock_stream(self, stream):
        for line in stream:
            self.logger.info(line.decode('utf-8'))

    monkeypatch.setattr(docker, 'from_env', lambda: MockDockerClient())
    monkeypatch.setattr(
        patched_cloudwatch.CloudwatchHandler.boto3,
        'Session',
        lambda **kwargs: MockSession(),
    )
    monkeypatch.setattr(
        patched_cloudwatch,
        'MAX_MESSAGE_BYTES',
        40,
    )
    monkeypatch.setattr(
        main.CloudwatchLogger,
        'stream_infinitely',
        mock_stream,
    )
    args = argparse.Namespace(
        docker_image='test_docker_image',
        bash_command='test_bash_command',
        aws_cloudwatch_group='test_aws_cloudwatch_group',
        aws_cloudwatch_stream='test_aws_cloudwatch_stream',
        aws_access_key_id='test_aws_access_key_id',
        aws_secret_access_key='test_aws_secret_access_key',
        aws_region='test_aws_region',
    )
    main.do_work(args)
    assert caplog.record_tuples == expected_caplog
    assert logs == expected_aws
