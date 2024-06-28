import argparse
import logging
import typing as tp

import botocore
import docker

import patched_cloudwatch


class DockerCloudwatchProblem(Exception):
    pass


class CloudwatchLogger:
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
        logger = logging.getLogger('docker_cloudwatch')
        formatter = logging.Formatter('%(asctime)s : %(levelname)s - %(message)s')
        try:
            cloudwatch_handler = patched_cloudwatch.CloudwatchHandler(
                log_group=log_group,
                log_stream=log_stream,
                access_id=access_id,
                access_key=access_key,
                region=region,
                overflow='split',
            )
        except botocore.exceptions.ClientError:
            raise DockerCloudwatchProblem(
                'Exception happened on cloudwatch setup, check your credentials.'
            )
        cloudwatch_handler.setFormatter(formatter)
        logger.setLevel(logging.INFO)
        logger.addHandler(cloudwatch_handler)
        self.logger = logger


    def stream_infinitely(
            self,
            stream: docker.types.daemon.CancellableStream,
    ):
        # cloudwatch lib uses boto3, which is synchronous.
        # If issues with speed arise - consider using aioboto3.
        # Major consideration: boto3 is supported by amazon, and aioboto3 is third-party,
        # so I decided in favor of a lightweight boto3 handler.
        try:
            while True:
                for line in stream:
                    self.logger.info(line.decode('utf-8'))
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
