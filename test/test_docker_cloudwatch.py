import argparse
import datetime
import logging
from unittest.mock import MagicMock
from unittest.mock import patch

import boto3
import docker
import pytest

import main
import cloudwatch_logger


@pytest.fixture
def time_fixture():
    with patch("time.time") as mock_time:
        mock_time.return_value = 1719484518
        yield mock_time


@pytest.mark.parametrize("container_logs,expected_aws,max_message_bytes", [
    # Happy path
    (
        b'1\n2\n3',
        [
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'logEvents': [{'timestamp': 1719484518000, 'message': '1'}, {'timestamp': 1719484518000, 'message': '2'}, {'timestamp': 1719484518000, 'message': '3'}]}
        ],
        1000,
    ),
    # Long message
    (
        b'10000000000000000000000000000000000000000',
        [
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'logEvents': {'timestamp': 1719484518000, 'message': '1000000000000000000000000000000000000000'}},
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'sequenceToken': 1, 'logEvents': {'timestamp': 1719484518000, 'message': '0'}}
        ],
        40,
    ),
    # Unicode
    (
        'Альфа бета гамма дельта'.encode('utf-8'),
        [
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'logEvents': {'timestamp': 1719484518000, 'message': 'Альфа бета гамма дель'}},
            {'logGroupName': 'test_aws_cloudwatch_group', 'logStreamName': 'test_aws_cloudwatch_stream', 'sequenceToken': 1, 'logEvents': {'timestamp': 1719484518000, 'message': 'та'}}
        ],
        40,
    ),
])
def test_docker_cloudwatch(
        monkeypatch,
        time_fixture,
        container_logs,
        expected_aws,
        max_message_bytes,
):
    logs = []
    token = 0
    logs_taken = False

    class MockContainer:
        def logs(self, since, until):
            nonlocal logs_taken
            if logs_taken:
                raise KeyboardInterrupt
            else:
                logs_taken = True
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

    monkeypatch.setattr(docker, 'from_env', lambda: MockDockerClient())
    monkeypatch.setattr(
        boto3,
        'Session',
        lambda **kwargs: MockSession(),
    )
    monkeypatch.setattr(
        cloudwatch_logger.CloudwatchLogger,
        'MAX_MESSAGE_BYTES',
        max_message_bytes,
    )
    args = argparse.Namespace(
        docker_image='test_docker_image',
        bash_command='test_bash_command',
        aws_cloudwatch_group='test_aws_cloudwatch_group',
        aws_cloudwatch_stream='test_aws_cloudwatch_stream',
        aws_access_key_id='test_aws_access_key_id',
        aws_secret_access_key='test_aws_secret_access_key',
        aws_region='test_aws_region',
        debug=False,
    )
    main.do_work(args)
    print(logs)
    assert logs == expected_aws
