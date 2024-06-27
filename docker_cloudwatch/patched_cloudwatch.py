import math

from cloudwatch import cloudwatch


MAX_MESSAGE_BYTES = cloudwatch.MAX_MESSAGE_BYTES


class CloudwatchHandler(cloudwatch.CloudwatchHandler):
    def emit(self, record):
        """This is the overriden function from the handler to send logs to AWS.
        Split overflow option is based on bytes, not characters, so it's possible
        that non-ASCII characters will end up being split.
        So take some extra effort on message splitting.
        """
        log_entry = self.format(record)

        #Check for event overflow and truncate (could otherwise add code to split in multiple events, if desired)
        current_overflow = len(log_entry.encode('utf-8')) - MAX_MESSAGE_BYTES
        #If no overflow, log it
        if current_overflow <= 0:
            self._send(log_entry)
        else:
            #If there is overflow check the behaviour
            if self.overflow == 'error':
                raise ValueError('Overflow: Message too large to handle in one API call. Please specify overflow behaviour to avoid this error, or reduce message size')
            elif self.overflow == 'truncate':
                #Truncate to MAX_MESSAGE_BYTES
                log_entry = log_entry.encode('utf-8')[:MAX_MESSAGE_BYTES].decode('utf-8', 'ignore')
                self._send(log_entry)
            elif self.overflow == 'split':
                # My fix starts here
                # This should send data with respect to unicode
                index = 0
                encoded_entry = log_entry.encode('utf-8')
                log_part = encoded_entry[:MAX_MESSAGE_BYTES].decode('utf-8', 'ignore')
                while log_part:
                    self._send(log_part)
                    index += len(log_part.encode('utf-8'))
                    log_part = encoded_entry[index:index+MAX_MESSAGE_BYTES].decode('utf-8', 'ignore')
                # My fix ends here
            else:
                raise KeyError('Unhandled overflow option')
