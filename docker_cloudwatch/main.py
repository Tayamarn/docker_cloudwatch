import argparse
import typing as tp

import botocore
import docker

import cloudwatch_logger


class DockerCloudwatchProblem(Exception):
    pass

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
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Print debug info.',
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


def do_work(args: argparse.Namespace):
    try:
        logger = cloudwatch_logger.CloudwatchLogger(
            log_group=args.aws_cloudwatch_group,
            log_stream=args.aws_cloudwatch_stream,
            access_id=args.aws_access_key_id,
            access_key=args.aws_secret_access_key,
            region=args.aws_region,
            debug=args.debug,
        )
    except botocore.exceptions.ClientError as error:
        if error.response['Error']['Code'] == 'InvalidSignatureException':
            raise DockerCloudwatchProblem(
                'Exception happened on cloudwatch setup, check your credentials.'
            )
        else:
            raise
    container = create_container(
        image_name=args.docker_image,
        bash_command=args.bash_command,
    )
    logger.stream_infinitely(container)


def main():
    args = parse_args()
    try:
        do_work(args)
    except DockerCloudwatchProblem as exc:
        print(exc)


if __name__ == '__main__':
    main()
