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

from __future__ import absolute_import

import argparse
import tornado.gen
import tornado.ioloop

from tchannel.tornado import TChannel
from tchannel.tornado.dispatch import RequestDispatcher


@tornado.gen.coroutine
def echo(request, response, proxy):
    response.set_header_s(request.get_header_s())
    response.set_body_s(request.get_body_s())


def main():  # pragma: no cover
    import sys

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--port",
        dest="port", default=8888, type=int,
    )
    parser.add_argument(
        "--host",
        dest="host", default="127.0.0.1"
    )
    args = parser.parse_args()

    client = TChannel('%s:%d' % (args.host, args.port))

    # TODO: service=test_as_raw
    handler = RequestDispatcher()
    client.host(handler)

    # TODO: do we need to implement these differently?
    handler.register("echo", echo)
    handler.register("streaming_echo", echo)

    client.listen()
    print 'listening on', client.hostport
    sys.stdout.flush()

    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':  # pragma: no cover
    main()
