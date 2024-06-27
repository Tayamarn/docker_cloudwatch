# Backend Engineer Test Task

Run example:
```
python main.py --docker-image python --bash-command $'pip install pip -U && pip
install tqdm && python -c \"import time\ncounter = 0\nwhile
True:\n\tprint(counter)\n\tcounter = counter + 1\n\ttime.sleep(0.1)\"'
--aws-cloudwatch-group test-task-group-1 --aws-cloudwatch-stream test-task-stream-1
--aws-access-key-id ... --aws-secret-access-key ... --aws-region ...
```

Program creates a Docker container using the given image name and bash command. After that, program handles the output logs of the container and sends them to AWS CloudWatch, using given group/stream and AWS credentials.


# Problems and decisions:
1) I used pypi packages `docker`, which is supported by Docker team and it covers all of my tasks.
2) I used `cloudwatch` package, which is a third-party logging handler, sending logs to CloudWatch. It is more or less updated by the maintainer.
3) CloudWatch has a maximum entry size of 256 KB (see https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-putevent-size.html ). I decided to split large messages in parts and send them separately, which can be difficult to read, but I suppose, such messages should be quite rare. Solution, proposed by AWS is uploading large messages to S3 bucket and including URL in logs.
4) I found a problem in `cloudwatch` handler - it splits messages with no respect to unicode (and, in fact, does not treat message splitting right), so I patched the problem method and included the patched version in file `patched_cloudwatch.py`.
5) `cloudwatch` uses boto3 library, which is synchronous. In case a problem with a very larcge amount of logs arises, one of the solutions could be rewriting the handler to `aioboto3`. A consideration against it - `boto3` is supported by AWS team and `aioboto3` is third-party. I decided in favor of existing handler.
6) I'm not removing the container because it could be needed for bug searching.
