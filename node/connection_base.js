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

var assert = require('assert');
var extend = require('xtend');
var inherits = require('util').inherits;
var EventEmitter = require('./lib/event_emitter');

var errors = require('./errors');
var States = require('./reqres_states');
var Operations = require('./operations');

var DEFAULT_OUTGOING_REQ_TIMEOUT = 2000;
var CONNECTION_BASE_IDENTIFIER = 0;

function TChannelConnectionBase(channel, direction, socketRemoteAddr) {
    assert(!channel.destroyed, 'refuse to create connection for destroyed channel');

    var self = this;
    EventEmitter.call(self);
    self.errorEvent = self.defineEvent('error');
    self.timedOutEvent = self.defineEvent('timedOut');
    self.spanEvent = self.defineEvent('span');
    self.pingResponseEvent = self.defineEvent('pingResonse');

    self.closing = false;
    self.closeError = null;
    self.closeEvent = self.defineEvent('close');

    self.channel = channel;
    self.options = self.channel.options;
    self.logger = channel.logger;
    self.random = channel.random;
    self.timers = channel.timers;
    self.direction = direction;
    self.socketRemoteAddr = socketRemoteAddr;
    self.timer = null;
    self.remoteName = null; // filled in by identify message

    self.ops = new Operations({
        timers: self.timers,
        logger: self.logger,
        random: self.random,
        initTimeout: self.channel.initTimeout,
        timeoutCheckInterval: self.options.timeoutCheckInterval,
        timeoutFuzz: self.options.timeoutFuzz,
        connectionStalePeriod: self.options.connectionStalePeriod,
        connection: self
    });

    self.guid = ++CONNECTION_BASE_IDENTIFIER;

    self.tracer = self.channel.tracer;
    self.ops.startTimeoutTimer();
}
inherits(TChannelConnectionBase, EventEmitter);

// create a request
TChannelConnectionBase.prototype.request = function connBaseRequest(options) {
    var self = this;
    if (!options) options = {};

    assert(self.remoteName, 'cannot make request unless identified');
    options.remoteAddr = self.remoteName;

    options.channel = self.channel;

    // TODO: use this to protect against >4Mi outstanding messages edge case
    // (e.g. zombie operation bug, incredible throughput, or simply very long
    // timeout
    // assert(!self.requests.out[id], 'duplicate frame id in flight');
    // TODO: provide some sort of channel default for "service"
    // TODO: generate tracing if empty?
    // TODO: refactor callers
    options.checksumType = options.checksum;

    // TODO: better default, support for dynamic
    options.ttl = options.timeout || DEFAULT_OUTGOING_REQ_TIMEOUT;
    options.tracer = self.tracer;
    var req = self.buildOutRequest(options);

    return self.ops.addOutReq(req);
};

TChannelConnectionBase.prototype.handleCallRequest = function handleCallRequest(req) {
    var self = this;
    
    self.ops.addInReq(req);

    req.remoteAddr = self.remoteName;
    req.errorEvent.on(onReqError);

    process.nextTick(runHandler);

    function onReqError(err) {
        self.onReqError(req, err);
    }

    function runHandler() {
        self.runHandler(req);
    }
};

TChannelConnectionBase.prototype.onReqError = function onReqError(req, err) {
    var self = this;
    if (!req.res) self.buildResponse(req);
    if (err.type === 'tchannel.timeout' ||
        err.type === 'tchannel.request.timeout'
    ) {
        req.res.sendError('Timeout', err.message);
    } else {
        var errName = err.name || err.constructor.name;
        req.res.sendError('UnexpectedError', errName + ': ' + err.message);
    }
};

TChannelConnectionBase.prototype.runHandler = function runHandler(req) {
    var self = this;
    self.channel.inboundCallsRecvdStat.increment(1, {
        'calling-service': req.headers.cn,
        'service': req.serviceName,
        'endpoint': String(req.arg1)
    });

    self.channel.handler.handleRequest(req, buildResponse);
    function buildResponse(options) {
        return self.buildResponse(req, options);
    }
};

TChannelConnectionBase.prototype.buildResponse = function buildResponse(req, options) {
    var self = this;
    var done = false;
    if (req.res && req.res.state !== States.Initial) {
        self.errorEvent.emit(self, errors.ResponseAlreadyStarted({
            state: req.res.state
        }));
    }
    options = extend({
        channel: self.channel,
        inreq: req
    }, options);
    req.res = self.buildOutResponse(req, options);
    req.res.errorEvent.on(onError);
    req.res.finishEvent.on(opDone);
    req.res.spanEvent.on(handleSpanFromRes);
    return req.res;

    function handleSpanFromRes(span) {
        self.spanEvent.emit(self, span);
    }

    function opDone() {
        if (done) return;
        done = true;
        self.onReqDone(req);
    }

    function onError(err) {
        self.onResponseError(err, req);
    }
};

function isStringOrBuffer(x) {
    return typeof x === 'string' || Buffer.isBuffer(x);
}

TChannelConnectionBase.prototype.onResponseError =
function onResponseError(err, req) {
    var self = this;

    var loggingOptions = {
        err: err,
        arg1: String(req.arg1),
        ok: req.res.ok,
        type: req.res.type,
        state: req.res.state === States.Done ? 'Done' :
            req.res.state === States.Error ? 'Error' :
            'Unknown'
    };

    if (req.res.state === States.Done) {
        var arg2 = isStringOrBuffer(req.res.arg2) ?
            req.res.arg2 : 'streaming';
        var arg3 = isStringOrBuffer(req.res.arg3) ?
            req.res.arg3 : 'streaming';

        loggingOptions.bufArg2 = arg2.slice(0, 50);
        loggingOptions.arg2 = String(arg2).slice(0, 50);
        loggingOptions.bufArg3 = arg3.slice(0, 50);
        loggingOptions.arg3 = String(arg3).slice(0, 50);
    } else if (req.res.state === States.Error) {
        loggingOptions.codeString = req.res.codeString;
        loggingOptions.errMessage = req.res.message;
    }

    if ((err.type === 'tchannel.response-already-started' ||
        err.type === 'tchannel.response-already-done') &&
        req.timedOut
    ) {
        self.logger.info(
            'error for timed out outgoing response', loggingOptions
        );
    } else {
        self.logger.error(
            'outgoing response has an error', loggingOptions
        );
    }
};

TChannelConnectionBase.prototype.onReqDone = function onReqDone(req) {
    var self = this;

    var inreq = self.ops.popInReq(req.id);

    // incoming req that timed out are already cleaned up
    if (inreq !== req && !req.timedOut) {
        self.logger.warn('mismatched onReqDone callback', {
            hostPort: self.channel.hostPort,
            hasInReq: inreq !== undefined,
            id: req.id
        });
    }
};

module.exports = TChannelConnectionBase;
