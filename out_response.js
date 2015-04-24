// Copyright (c) 2015 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

'use strict';

var EventEmitter = require('events').EventEmitter;
var inherits = require('util').inherits;

var errors = require('./errors');
var States = require('./reqres_states');

function TChannelOutResponse(id, options) {
    options = options || {};
    var self = this;
    EventEmitter.call(self);
    self.logger = options.logger;
    self.random = options.random;
    self.timers = options.timers;

    self.start = 0;
    self.end = 0;
    self.state = States.Initial;
    self.id = id || 0;
    self.code = options.code || 0;
    self.tracing = options.tracing || null;
    self.headers = options.headers || {};
    self.checksumType = options.checksumType || 0;
    self.checksum = options.checksum || null;
    self.ok = self.code === 0;
    self.span = options.span || null;
    self.streamed = false;
    self._argstream = null;
    self.arg1 = null;
    self.arg2 = null;
    self.arg3 = null;

    self.on('finish', self.onFinish);
}

inherits(TChannelOutResponse, EventEmitter);

TChannelOutResponse.prototype.type = 'tchannel.outgoing-response';

TChannelOutResponse.prototype._sendCallResponse = function _sendCallResponse(args, isLast) {
    var self = this;
    throw errors.UnimplementedMethod({
        className: self.constructor.name,
        methodName: '_sendCallResponse'
    });
};

TChannelOutResponse.prototype._sendCallResponseCont = function _sendCallResponseCont(args, isLast) {
    var self = this;
    throw errors.UnimplementedMethod({
        className: self.constructor.name,
        methodName: '_sendCallResponseCont'
    });
};

TChannelOutResponse.prototype._sendError = function _sendError(codeString, message) {
    var self = this;
    throw errors.UnimplementedMethod({
        className: self.constructor.name,
        methodName: '_sendError'
    });
};

TChannelOutResponse.prototype.onFinish = function onFinish() {
    var self = this;
    if (!self.end) self.end = self.timers.now();
    if (self.span) {
        self.emit('span', self.span);
    }
};

TChannelOutResponse.prototype.sendParts = function sendParts(parts, isLast) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            self.sendCallResponseFrame(parts, isLast);
            break;
        case States.Streaming:
            self.sendCallResponseContFrame(parts, isLast);
            break;
        case States.Done:
            self.emit('error', errors.ResponseFrameState({
                attempted: 'arg parts',
                state: 'Done'
            }));
            break;
        case States.Error:
            // TODO: log warn
            break;
    }
};

TChannelOutResponse.prototype.sendCallResponseFrame = function sendCallResponseFrame(args, isLast) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            self.start = self.timers.now();
            self._sendCallResponse(args, isLast);
            if (self.span) {
                self.span.annotate('ss');
            }
            if (isLast) self.state = States.Done;
            else self.state = States.Streaming;
            break;
        case States.Streaming:
            self.emit('error', errors.ResponseFrameState({
                attempted: 'call response',
                state: 'Streaming'
            }));
            break;
        case States.Done:
        case States.Error:
            self.emit('error', errors.ResponseAlreadyDone({
                attempted: 'call response'
            }));
    }
};

TChannelOutResponse.prototype.sendCallResponseContFrame = function sendCallResponseContFrame(args, isLast) {
    var self = this;
    switch (self.state) {
        case States.Initial:
            self.emit('error', errors.ResponseFrameState({
                attempted: 'call response continuation',
                state: 'Initial'
            }));
            break;
        case States.Streaming:
            self._sendCallResponseCont(args, isLast);
            if (isLast) self.state = States.Done;
            break;
        case States.Done:
        case States.Error:
            self.emit('error', errors.ResponseAlreadyDone({
                attempted: 'call response continuation'
            }));
    }
};

TChannelOutResponse.prototype.sendError = function sendError(codeString, message) {
    var self = this;
    if (self.state === States.Done || self.state === States.Error) {
        self.emit('error', errors.ResponseAlreadyDone({
            attempted: 'send error frame: ' + codeString + ': ' + message
        }));
    } else {
        if (self.span) {
            self.span.annotate('ss');
        }
        self.state = States.Error;
        self._sendError(codeString, message);
        self.emit('finish');
    }
};

TChannelOutResponse.prototype.setOk = function setOk(ok) {
    var self = this;
    if (self.state !== States.Initial) {
        self.emit('error', errors.ResponseAlreadyStarted({
            state: self.state
        }));
    }
    self.ok = ok;
    self.code = ok ? 0 : 1; // TODO: too coupled to v2 specifics?
};

TChannelOutResponse.prototype.sendOk = function sendOk(res1, res2) {
    var self = this;
    self.setOk(true);
    self.send(res1, res2);
};

TChannelOutResponse.prototype.sendNotOk = function sendNotOk(res1, res2) {
    var self = this;
    if (self.state === States.Error) {
        self.logger.error('cannot send application error, already sent error frame', {
            res1: res1,
            res2: res2
        });
    } else {
        self.setOk(false);
        self.send(res1, res2);
    }
};

TChannelOutResponse.prototype.send = function send(res1, res2) {
    var self = this;
    self.sendCallResponseFrame([self.arg1, res1, res2], true);
    self.emit('finish');
};

module.exports = TChannelOutResponse;