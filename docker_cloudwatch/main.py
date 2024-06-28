import argparse
import boto3
import logging
import time
import typing as tp

import botocore
import docker


class DockerCloudwatchProblem(Exception):
    pass


class CloudwatchLogger:
    #SET THE max message size (- 26 for overhead)
    MAX_MESSAGE_BYTES = 256 * 1024 - 26

    def __init__(
            self,
            log_group: str,
            log_stream: str,
            access_id: str,
            access_key: str,
            region: str,
    ):
        self._setup_logger(
            log_group=log_group,
            log_stream=log_stream,
            access_id=access_id,
            access_key=access_key,
            region=region,
        )

    def _setup_logger(
            self,
            log_group: str,
            log_stream: str,
            access_id: str,
            access_key: str,
            region: str,
    ) -> tp.Optional[logging.Logger]:
        self.log_group = log_group
        self.log_stream = log_stream
        self.access_id = access_id
        self.access_key = access_key
        self.region = region
        self.next_sequence_token = None

        self.session = boto3.Session(
            aws_access_key_id=access_id,
            aws_secret_access_key=access_key,
            region_name=region
        )
        self.logs_client = self.session.client('logs')
        try:
            response = self.logs_client.describe_log_streams(
                logGroupName=self.log_group,
            )
            for l in response['logStreams']:
                if l['logStreamName'] == self.log_stream:
                    self.next_sequence_token = l['uploadSequenceToken'] if 'uploadSequenceToken' in l else None
            if self.next_sequence_token is None:
                self.logs_client.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream,
                )
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                pass
            elif error.response['Error']['Code'] == 'ResourceNotFoundException':
                self.logs_client.create_log_group(
                    logGroupName=self.log_group,
                )
                self.logs_client.create_log_stream(
                    logGroupName=self.log_group,
                    logStreamName=self.log_stream,
                )
            else:
                raise

    def actually_send_log_batch(self, entries: dict):
        print(entries)
        if self.next_sequence_token:
            response = self.logs_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                sequenceToken = self.next_sequence_token,
                logEvents=entries,
            )
        else:
            response = self.logs_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=entries,
            )
        self.next_sequence_token = response['nextSequenceToken']

    def send_log_batch(self, entries: dict):
        try:
          self.actually_send_log_batch(entries)
        except botocore.exceptions.ClientError as error:
            # Not the best way to get tokens, but I got this from another project.
            if error.response['Error']['Code'] == 'DataAlreadyAcceptedException':
                # Ignore DataAlreadyAcceptedException and get next token
                exception_text = str(e)
                self.next_sequence_token = exception_text[exception_text.find("sequenceToken: ")+15:]
            elif error.response['Error']['Code'] == 'InvalidSequenceTokenException':
                # Change the token and retry
                exception_text = str(e)
                self.next_sequence_token = exception_text[exception_text.find("sequenceToken is: ")+18:]
                self.send_log(timestamp, log_entry)
            else:
                # Wait and try to resend
                time.sleep(1)
                self.actually_send_log_batch(entries)

    def generate_entry(self, timestamp: int, message: str):
        return {
            'timestamp': timestamp,
            'message': message
        }

    def stream_infinitely(
            self,
            stream: docker.types.daemon.CancellableStream,
    ):
        entries = []
        entries_size = 0
        try:
            while True:
                timestamp = round(time.time() * 1000)
                entries = []
                entries_size = 0
                for line in stream:
                    # 45 for overhead
                    entry_size = len(line) + 45
                    if entries_size + entry_size > self.MAX_MESSAGE_BYTES:
                        self.send_log_batch(entries)
                        entries = []
                        entries_size = 0

                    if entry_size > self.MAX_MESSAGE_BYTES:
                        index = 0
                        encoded_entry = log_entry.encode('utf-8')
                        log_part = encoded_entry[:MAX_MESSAGE_BYTES].decode('utf-8', 'ignore')
                        while log_part:
                            self.send_log_batch(self.generate_entry(timestamp, log_part))
                            index += len(log_part.encode('utf-8'))
                            log_part = encoded_entry[index:index+MAX_MESSAGE_BYTES].decode('utf-8', 'ignore')
                    else:
                        entry = self.generate_entry(timestamp, line.decode('utf-8'))
                        entries.append(entry)
                        entries_size += entry_size
                if entries:
                    self.send_log_batch(entries)

        except KeyboardInterrupt:
            print('Keyboard Interrupt, exiting.')
        finally:
            if entries:
                self.send_log_batch(entries)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'Run a Docker container using the given image name and bash command. '
            'Send output logs to Cloudwatch, using the provided credentials.'
        ),
    )
    parser.add_argument(
        '--docker-image',
        required=True,
        help='Name of a Docker image.',
    )
    parser.add_argument(
        '--bash-command',
        required=True,
        help='Bash command to run inside the Docker container.',
    )
    parser.add_argument(
        '--aws-cloudwatch-group',
        required=True,
        help='Name of an AWS CloudWatch group.',
    )
    parser.add_argument(
        '--aws-cloudwatch-stream',
        required=True,
        help='Name of an AWS CloudWatch stream.',
    )
    parser.add_argument(
        '--aws-access-key-id',
        required=True,
        help='AWS access key.',
    )
    parser.add_argument(
        '--aws-secret-access-key',
        required=True,
        help='AWS secret access key.',
    )
    parser.add_argument(
        '--aws-region',
        required=True,
        help='AWS region name.',
    )
    return parser.parse_args()


def get_image(
        client: docker.client.DockerClient,
        docker_image: str,
) -> tp.Optional[docker.models.images.Image]:
    try:
        image = client.images.get(name=docker_image)
    except docker.errors.ImageNotFound:
        try:
            image = client.images.pull(docker_image)
        except docker.errors.ImageNotFound:
            raise DockerCloudwatchProblem(f'Cannot find image {docker_image}.')
    return image


def create_container(
        image_name: str,
        bash_command: str,
) -> docker.models.containers.Container:
    client = docker.from_env()
    image = get_image(client, image_name)
    return client.containers.run(
        image,
        command=['bash', '-c', bash_command],
        detach=True
    )


def send_container_logs_to_logger(
        container: docker.models.containers.Container,
        logger: CloudwatchLogger,
):
    logs = container.logs(stream=True)
    logger.stream_infinitely(logs)


def do_work(args: argparse.Namespace):
    cloudwatch_logger = CloudwatchLogger(
        log_group=args.aws_cloudwatch_group,
        log_stream=args.aws_cloudwatch_stream,
        access_id=args.aws_access_key_id,
        access_key=args.aws_secret_access_key,
        region=args.aws_region,
    )
    container = create_container(
        image_name=args.docker_image,
        bash_command=args.bash_command,
    )
    send_container_logs_to_logger(container, cloudwatch_logger)


def main():
    args = parse_args()
    try:
        do_work(args)
    except DockerCloudwatchProblem as exc:
        print(exc)


if __name__ == '__main__':
    main()
