const { Kafka, logLevel, CompressionTypes, CompressionCodecs } = require('kafkajs');
const LZ4 = require('lz4');

const kafka = new Kafka({
    clientId: 'webapp',
    brokers: ['kafka:9092'],
    logLevel: logLevel.INFO,
    connectionTimeout: 10000,
    requestTimeout: 25000,
});

const consumer = kafka.consumer({ groupId: 'webapp-consumer-group' });

const LZ4Codec = {
    async compress(buffer) {
        return LZ4.encode(buffer);
    },
    async decompress(buffer) {
        return LZ4.decode(buffer);
    },
};

CompressionCodecs[CompressionTypes.LZ4] = () => LZ4Codec;

module.exports = { kafka, consumer };