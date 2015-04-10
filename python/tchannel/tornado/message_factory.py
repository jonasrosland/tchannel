# Copyright (c) 2015 Uber Technologies, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

from ..exceptions import StreamingException
from ..messages import Types, RW
from ..messages.call_request import CallRequestMessage
from ..messages.call_request_continue import CallRequestContinueMessage
from ..messages.call_response_continue import CallResponseContinueMessage
from ..messages.call_response import CallResponseMessage
from ..messages.call_continue import CallContinueMessage
from ..messages import common
from ..messages.common import StreamState, FlagsType
from .dispatch import Request, Response
from .stream import InMemStream


class MessageFactory(object):
    """Provide the functionality to decompose and recompose
    streaming messages.
    """

    def __init__(self):
        # key: message_id
        # value: incomplete streaming messages
        self.message_buffer = {}

    def build_raw_request_message(self, request, args, is_completed=False):
        """build protocol level message based on request and args.

        request object contains meta information about outgoing request.
        args are the currently chunk data from argstreams
        is_completed tells the flags of the message

        :param request: Request
        :param args: array of arg streams
        :param is_completed: message flags
        :return: CallRequestMessage/CallRequestContinueMessage
        """
        request.flags = FlagsType.none if is_completed else FlagsType.fragment

        # TODO decide what need to pass from request
        if request.state == StreamState.init:
            message = CallRequestMessage(
                flags=request.flags,
                # ttl=request.ttl,
                # tracing=request.tracing,
                service=request.service,
                # headers=request.headers,
                # checksum=request.checksum
                args=args
            )
            request.state = (StreamState.completed if is_completed
                             else StreamState.streaming)
        elif request.state == StreamState.streaming:
            message = CallRequestContinueMessage(
                flags=request.flags,
                # checksum=request.checksum
                args=args
            )
            request.state = (StreamState.completed if is_completed
                             else StreamState.streaming)
        else:
            raise StreamingException("request state Error")

        return message

    def build_raw_response_message(self, response, args, is_completed=False):
        """build protocol level message based on response and args.

        response object contains meta information about outgoing response.
        args are the currently chunk data from argstreams
        is_completed tells the flags of the message

        :param response: Response
        :param args: array of arg streams
        :param is_completed: message flags
        :return: CallResponseMessage/CallResponseContinueMessage
        """
        response.flags = FlagsType.none if is_completed else FlagsType.fragment

        # TODO decide what need to pass from request
        if response.state == StreamState.init:
            message = CallResponseMessage(
                flags=response.flags,
                # code=response.code,
                # tracing=response.tracing,
                # headers=response.headers,
                # checksum=response.checksum
                args=args
            )
            response.state = (StreamState.completed if is_completed
                              else StreamState.streaming)
        elif response.state == StreamState.streaming:
            message = CallResponseContinueMessage(
                flags=response.flags,
                # checksum=response.checksum
                args=args
            )
            response.state = (StreamState.completed if is_completed
                              else StreamState.streaming)
        else:
            raise StreamingException("response state Error")

        return message

    def build_raw_message(self, context, args, is_completed=False):
        if isinstance(context, Request):
            return self.build_raw_request_message(context, args, is_completed)
        elif isinstance(context, Response):
            return self.build_raw_response_message(context, args, is_completed)
        else:
            raise StreamingException("context object type error")

    def build_request(self, message, message_id=None):
        """Build request object from protocol level message info

        It is allowed to take incompleted CallRequestMessage. Therefore the
        created request may not contain whole three arguments.

        :param message: CallRequestMessage
        :param message_id: integer of message id
        :return: request object
        """
        args = []
        for arg in message.args:
            args.append(InMemStream(arg, auto_close=False))

        # TODO decide what to pass to Request from message
        req = Request(
            flags=message.flags,
            # ttl=message.ttl,
            # tracing=message.tracing,
            service=message.service,
            # headers=messaget.headers,
            # checksum=message.checksum
            argstreams=args,
            id=message_id
        )
        return req

    def build_response(self, message, message_id=None):
        """Build response object from protocol level message info

        It is allowed to take incompleted CallResponseMessage. Therefore the
        created request may not contain whole three arguments.

        :param message: CallResponseMessage
        :param message_id: integer of message id
        :return: response object
        """

        args = []
        for arg in message.args:
            args.append(InMemStream(arg, auto_close=False))

        # TODO decide what to pass to Response from message
        res = Response(
            flags=message.flags,
            # code=message.code,
            # tracing=message.tracing,
            # headers=messaget.headers,
            # checksum=message.checksum
            argstreams=args,
            id=message_id
        )
        return res

    def build_context(self, context, message_id=None):
        if context.message_type == Types.CALL_REQ:
            return self.build_request(context, message_id)
        elif context.message_type == Types.CALL_RES:
            return self.build_response(context, message_id)
        else:
            raise StreamingException("invalid message type: %s" %
                                     context.message_type)

    def build(self, message_id, message):
        """buffer all the streaming messages based on the
        message id. Reconstruct all fragments together.

        :param message_id:
            id
        :param message:
            incoming message
        :return: next complete message or None if streaming
            is not done
        """
        context = None
        if message.message_type in [Types.CALL_REQ,
                                    Types.CALL_RES]:

            context = self.build_context(message, message_id)
            # streaming message
            if message.flags == common.FlagsType.fragment:
                self.message_buffer[message_id] = context

            self.close_argstream(context)
            return context

        elif message.message_type in [Types.CALL_REQ_CONTINUE,
                                      Types.CALL_RES_CONTINUE]:
            context = self.message_buffer.get(message_id)
            if context is None:
                # missing call msg before continue msg
                raise StreamingException(
                    "missing call message after receiving continue message")

            dst = len(context.argstreams) - 1
            src = 0
            while src < len(message.args):
                if dst < len(context.argstreams):
                    context.argstreams[dst].write(message.args[src])
                else:
                    # only build InMemStream internally
                    new_stream = InMemStream(auto_close=False)
                    new_stream.write(message.args[src])
                    context.argstreams.append(new_stream)

                dst += 1
                src += 1
            if message.flags != FlagsType.fragment:
                # get last fragment. mark it as completed
                assert (len(context.argstreams) ==
                        CallContinueMessage.max_args_num)
                self.message_buffer.pop(message_id, None)
                context.flags = FlagsType.none

            self.close_argstream(context)
            return None
        else:
            # TODO build error response or request object
            return message

    @staticmethod
    def fragment(message):
        """Fragment message based on max payload size

        note: if the message doesn't need to fragment,
        it will return a list which only contains original
        message itself.

        :param message: raw message
        :return: list of messages whose sizes <= max
            payload size
        """
        if message.message_type in [Types.CALL_RES,
                                    Types.CALL_REQ,
                                    Types.CALL_REQ_CONTINUE,
                                    Types.CALL_RES_CONTINUE]:

            rw = RW[message.message_type]
            payload_space = (common.MAX_PAYLOAD_SIZE -
                             rw.length_no_args(message))
            # split a call/request message into an array
            # with a call/request message and {0~n} continue
            # message
            fragment_msg = message.fragment(payload_space)
            yield message
            while fragment_msg is not None:
                message = fragment_msg
                rw = RW[message.message_type]
                payload_space = (common.MAX_PAYLOAD_SIZE -
                                 rw.length_no_args(message))
                fragment_msg = message.fragment(payload_space)
                yield message
        else:
            yield message

    @staticmethod
    def close_argstream(request):
        # close the stream for completed args since we have received all
        # the chunks
        if request.flags == FlagsType.none:
            num = len(request.argstreams)
        else:
            num = len(request.argstreams) - 1
        for i in range(num):
            request.argstreams[i].close()
