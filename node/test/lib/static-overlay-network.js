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

var NullStatsd = require('uber-statsd-client/null');
var debugLogtron = require('debug-logtron');
var inherits = require('util').inherits;
var allocCluster = require('./alloc-cluster.js');
var EventEmitter = require('../../lib/event_emitter.js');
var EndpointHandler = require('../../endpoint-handler.js');
var ServiceProxy = require('../../hyperbahn/service_proxy.js');
var RelayHandler = require('../../relay_handler.js');

function noop() {}

function NullConfig() {}
NullConfig.prototype.get = function get(key) {};

// e.g., topology = {'alice': ['127.0.0.1:4040'], 'bob': ['127.0.0.1:4041']}, '127.0.0.1:4040'
function FakeEgressNodes(topology, hostPort) {
    var self = this;
    EventEmitter.call(self);
    self.hostPort = hostPort;
    self.topology = topology;
    self.membershipChangedEvent = self.defineEvent('membershipChanged');
}

inherits(FakeEgressNodes, EventEmitter);

FakeEgressNodes.prototype.isExitFor = function isExitFor(serviceName) {
    var self = this;
    return self.topology[serviceName].indexOf(self.hostPort) >= 0;
};

FakeEgressNodes.prototype.exitsFor = function exitsFor(serviceName) {
    var self = this;
    var hostPorts = self.topology[serviceName];
    var exitsFor = Object.create(null);
    hostPorts.forEach(function (hostPort) {
        exitsFor[hostPort] = serviceName + "~1";
    });
    return exitsFor;
};

function StaticOverlayNetwork(options) {
    var self = this;

    self.numRelays = options.numRelays || 3;
    self.numInstancesPerService = options.numInstancesPerService || 3;
    self.serviceNames = options.serviceNames || ['alice', 'bob', 'charlie'];
    self.createCircuits = options.createCircuits || noop;

    self.numPeers = self.numRelays + self.serviceNames.length * self.numInstancesPerService;
    self.cluster = null;
    self.topology = null;
    self.relayChannels = null;
    self.serviceChannels = null;
    self.serviceChannelsByName = null;

    var relayIndexes = [];
    for (var relayIndex = 0; relayIndex < self.numRelays; relayIndex++) {
        relayIndexes.push(relayIndex);
    }
    self.relayIndexes = relayIndexes;

    var instanceIndexes = [];
    for (var instanceIndex = 0; instanceIndex < self.numInstancesPerService; instanceIndex++) {
        instanceIndexes.push(instanceIndex);
    }
    self.instanceIndexes = instanceIndexes;

}

StaticOverlayNetwork.test = function test(name, options, callback) {
    var network = new StaticOverlayNetwork(options);
    var clusterOptions = options.cluster || {};
    clusterOptions.numPeers = network.numPeers;
    allocCluster.test(name, clusterOptions, onCluster);
    function onCluster(cluster, assert) {
        network.setCluster(cluster);
        callback(network, assert);
    }
};

StaticOverlayNetwork.prototype.setCluster = function setCluster(cluster) {
    var self = this;
    self.cluster = cluster;

    // consume channels for the following services
    var nextChannelIndex = 0;

    self.relayChannels = self.relayIndexes.map(function () {
        return cluster.channels[nextChannelIndex++];
    });

    self.serviceChannels = [];
    self.serviceChannelsByName = {};
    self.serviceNames.forEach(function (serviceName) {
        var channels = self.instanceIndexes.map(function (instanceIndex) {
            return cluster.channels[nextChannelIndex++];
        });
        self.serviceChannels.push(channels);
        self.serviceChannelsByName[serviceName] = channels;
    });

    // Create a relay topology for egress nodes.
    self.topology = {};
    self.serviceChannels.forEach(function (channels, index) {
        var serviceName = self.serviceNames[index];
        var relayHostPorts = [];
        relayHostPorts.push(self.relayChannels[(index + 0) % self.relayChannels.length].hostPort);
        relayHostPorts.push(self.relayChannels[(index + 1) % self.relayChannels.length].hostPort);
        self.topology[serviceName] = relayHostPorts;
    });

    self.egressNodesForRelay = self.relayChannels.map(function eachRelay(relayChannel, index) {
        return new FakeEgressNodes(self.topology, relayChannel.hostPort);
    });

    // Set up relays
    self.relayChannels.forEach(function (relayChannel, index) {
        var egressNodes = self.egressNodesForRelay[index];
        var config = new NullConfig();
        var statsd = new NullStatsd();

        relayChannel.handler = new ServiceProxy({
            channel: relayChannel,
            logger: debugLogtron('serviceproxy' + index, {enabled: false}),
            config: config,
            statsd: statsd,
            egressNodes: egressNodes,
            circuits: self.createCircuits()
        });

        // In response to artificial advertisement
        self.serviceNames.forEach(function eachServiceName(serviceName, index) {
            if (egressNodes.isExitFor(serviceName)) {
                self.serviceChannels[index].forEach(function (serviceChannel) {
                    relayChannel.handler.getServicePeer(serviceName, serviceChannel.hostPort);
                });
            }
        });
    });

    // Create and connect service channels
    self.subChannels = [];
    self.subChannelsByServiceName = {};
    self.serviceChannels.forEach(function (channels, serviceIndex) {
        var serviceName = self.serviceNames[serviceIndex];
        var subChannels = channels.map(function (channel, channelIndex) {
            var subChannel = channel.makeSubChannel({serviceName: serviceName});

            // Set up server
            var endpointHandler = new EndpointHandler(serviceName);
            endpointHandler.register('echo', function (req, res) {
                res.sendOk(req.arg2, req.arg3);
            });
            subChannel.handler = endpointHandler;

            return subChannel;
        });
        self.subChannels.push(subChannels);
        self.subChannelsByServiceName[serviceName] = subChannels;
    });

};

StaticOverlayNetwork.prototype.forEachSubChannel = function (callback) {
    var self = this;
    self.subChannels.forEach(function (subChannels, serviceIndex) {
        var serviceName = self.serviceNames[serviceIndex];
        subChannels.forEach(function (subChannel, instanceIndex) {
            callback(subChannel, serviceName, instanceIndex);
        });
    });
};

module.exports = StaticOverlayNetwork;
