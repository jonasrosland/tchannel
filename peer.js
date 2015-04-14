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

var inherits = require('util').inherits;
var EventEmitter = require('events').EventEmitter;
var net = require('net');

var TChannelConnection = require('./connection');
var TChannelPeerHealthyState = require('./peer_healthy_state');

function TChannelPeer(channel, hostPort, options) {
    if (!(this instanceof TChannelPeer)) {
        return new TChannelPeer(channel, hostPort, options);
    }
    var self = this;
    EventEmitter.call(self);
    self.channel = channel;
    self.logger = self.channel.logger;
    self.options = options || {};
    self.hostPort = hostPort;
    self.isEphemeral = self.hostPort === '0.0.0.0:0';
    self.state = null; // TODO
    self.connections = [];
    if (self.options.initialState) {
        self.setState(self.options.initialState);
        delete self.options.initialState;
    } else {
        self.setState(TChannelPeerHealthyState);
    }
}

inherits(TChannelPeer, EventEmitter);

TChannelPeer.prototype.isConnected = function isConnected(direction, identified) {
    var self = this;
    if (identified === undefined) identified = true;
    for (var i = 0; i < self.connections.length; i++) {
        var conn = self.connections[i];
        if (direction && conn.direction !== direction) {
            continue;
        } else if (conn.closing) {
            continue;
        } else if (conn.remoteName !== null || !identified) {
            return true;
        }
    }
    return false;
};

TChannelPeer.prototype.close = function close(callback) {
    var self = this;
    var counter = self.connections.length;
    if (counter) {
        self.connections.forEach(function eachConn(conn) {
            conn.close(onClose);
        });
    } else {
        self.state.close(callback);
    }
    function onClose() {
        if (--counter <= 0) {
            if (counter < 0) {
                self.logger.error('closed more peer sockets than expected', {
                    counter: counter
                });
            }
            self.state.close(callback);
        }
    }
};

TChannelPeer.prototype.setState = function setState(StateType) {
    var self = this;
    var currentName = self.state && self.state.name;
    if (currentName &&
        StateType.prototype.name &&
        StateType.prototype.name === currentName) {
        return;
    }
    var state = StateType(self.channel, self);
    if (state && state.name === currentName) {
        return;
    }
    var oldState = self.state;
    self.state = state;
    self.emit('stateChanged', oldState, state);
};

TChannelPeer.prototype.getInConnection = function getInConnection() {
    var self = this;
    for (var i = 0; i < self.connections.length; i++) {
        var conn = self.connections[i];
        if (!conn.closing) return conn;
    }
    return null;
};

TChannelPeer.prototype.getOutConnection = function getOutConnection() {
    var self = this;
    for (var i = self.connections.length - 1; i >= 0; i--) {
        var conn = self.connections[i];
        if (!conn.closing) return conn;
    }
    return null;
};

TChannelPeer.prototype.connect = function connect(outOnly) {
    var self = this;
    var conn = self.getOutConnection();
    if (!conn || (outOnly && conn.direction !== 'out')) {
        var socket = self.makeOutSocket();
        conn = self.makeOutConnection(socket);
        self.addConnection(conn);
    }
    return conn;
};

TChannelPeer.prototype.request = function peerRequest(options) {
    var self = this;
    return self.connect().request(options);
};

TChannelPeer.prototype.addConnection = function addConnection(conn) {
    var self = this;
    // TODO: first approx alert for self.connections.length > 2
    // TODO: second approx support pruning
    if (conn.direction === 'out') {
        self.connections.push(conn);
    } else {
        self.connections.unshift(conn);
    }
    conn.once('error', onConnectionError);
    return conn;

    function onConnectionError(/* err */) {
        // TODO: log?
        self.removeConnection(conn);
    }
};

TChannelPeer.prototype.removeConnection = function removeConnection(conn) {
    var self = this;
    var list = self.connections;
    var index = list ? list.indexOf(conn) : -1;
    if (index !== -1) {
        return list.splice(index, 1)[0];
    } else {
        return null;
    }
};

TChannelPeer.prototype.makeOutSocket = function makeOutSocket() {
    var self = this;
    var parts = self.hostPort.split(':');
    if (parts.length !== 2) {
        throw new Error('invalid destination'); // TODO typed error
    }
    var host = parts[0];
    var port = parts[1];
    if (host === '0.0.0.0' || port === '0') {
        throw new Error('cannot make out connection to ephemeral peer'); // TODO typed error
    }
    var socket = net.createConnection({host: host, port: port});
    return socket;
};

TChannelPeer.prototype.makeOutConnection = function makeOutConnection(socket) {
    var self = this;
    var chan = self.channel.topChannel || self.channel;
    var conn = new TChannelConnection(chan, socket, 'out', self.hostPort);
    self.emit('allocConnection', conn);
    return conn;
};

module.exports = TChannelPeer;