import argparse
import asyncio
import logging
import typing as tp

import aioboto3
import aiobotocore
import botocore
import docker

# import patched_cloudwatch


class DockerCloudwatchProblem(Exception):
    pass


class CloudwatchLogger:
    MAX_MESSAGE_BYTES = 256 * 1024 - 26

    @classmethod
    async def create(
            cls,
            log_group: str,
            log_stream: str,
            access_id: str,
            access_key: str,
            region: str,
    ):
        self = cls()
        await self._setup_logger(
            log_group=log_group,
            log_stream=log_stream,
            access_id=access_id,
            access_key=access_key,
            region=region,
        )
        return self

    async def _setup_logger(
            self,
            log_group: str,
            log_stream: str,
            access_id: str,
            access_key: str,
            region: str,
    ) -> tp.Optional[logging.Logger]:
        # logger = logging.getLogger('docker_cloudwatch')
        # formatter = logging.Formatter('%(asctime)s : %(levelname)s - %(message)s')
        # try:
        #     cloudwatch_handler = patched_cloudwatch.CloudwatchHandler(
        #         log_group=log_group,
        #         log_stream=log_stream,
        #         access_id=access_id,
        #         access_key=access_key,
        #         region=region,
        #         overflow='split',
        #     )
        # except botocore.exceptions.ClientError:
        #     raise DockerCloudwatchProblem(
        #         'Exception happened on cloudwatch setup, check your credentials.'
        #     )
        # cloudwatch_handler.setFormatter(formatter)
        # logger.setLevel(logging.INFO)
        # logger.addHandler(cloudwatch_handler)
        # self.logger = logger
        self.log_group = log_group
        self.log_stream = log_stream
        self.access_id = access_id
        self.access_key = access_key
        self.region = region
        self.next_sequence_token = None
        self.semaphore = asyncio.Semaphore(20)

        self.session = aioboto3.Session(
            aws_access_key_id=access_id,
            aws_secret_access_key=access_key,
            region_name=region
        )
        async with self.session.client('logs') as logs_client:
            try:
                response = await logs_client.describe_log_streams(
                    logGroupName=self.log_group,
                )
                for l in response['logStreams']:
                    if l['logStreamName'] == self.log_stream:
                        self.next_sequence_token = l['uploadSequenceToken'] if 'uploadSequenceToken' in l else None
                if self.next_sequence_token is None:
                    await logs_client.create_log_stream(
                        logGroupName=self.log_group,
                        logStreamName=self.log_stream,
                    )
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'ResourceAlreadyExistsException':
                    pass
                elif error.response['Error']['Code'] == 'ResourceNotFoundException':
                    await logs_client.create_log_group(
                        logGroupName=self.log_group,
                    )
                    await logs_client.create_log_stream(
                        logGroupName=self.log_group,
                        logStreamName=self.log_stream,
                    )
                else:
                    raise


    async def send_line(
        self,
        logs_client: aiobotocore.client.CloudWatchLogs,
        log_entry: str,
        timestamp: int,
    ):
        async with self.semaphore:
            # Send the message to AWS (function depends if there is a token or not)
            if self.next_sequence_token:
              response = logs_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                sequenceToken = self.next_sequence_token,
                logEvents=[{'timestamp': timestamp,'message': log_entry}])
            else:
              response = logs_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=[{'timestamp': timestamp,'message': log_entry}])

            #Store the next token
            self.next_sequence_token = response['nextSequenceToken']

    async def stream_infinitely(
            self,
            stream: docker.types.daemon.CancellableStream,
    ):
        async with self.session.client('logs') as logs_client:
            try:
                while True:
                    pass
                    # tasks =
                    # for line in stream:
                        # self.logger.info(line.decode('utf-8'))
            except KeyboardInterrupt:
                print('Keyboard Interrupt, exiting.')


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


async def send_container_logs_to_logger(
        container: docker.models.containers.Container,
        logger: CloudwatchLogger,
):
    logs = container.logs(stream=True)
    await logger.stream_infinitely(logs)


async def do_work(args: argparse.Namespace):
    cloudwatch_logger = await CloudwatchLogger.create(
        log_group=args.aws_cloudwatch_group,
        log_stream=args.aws_cloudwatch_stream,
        access_id=args.aws_access_key_id,
        access_key=args.aws_secret_access_key,
        region=args.aws_region,
    )
    return
    container = create_container(
        image_name=args.docker_image,
        bash_command=args.bash_command,
    )
    await send_container_logs_to_logger(container, cloudwatch_logger)


async def main():
    args = parse_args()
    try:
        await do_work(args)
    except DockerCloudwatchProblem as exc:
        print(exc)


if __name__ == '__main__':
    asyncio.run(main())
