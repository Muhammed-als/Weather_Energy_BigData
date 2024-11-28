const avro = require('avro-js');
const { consumer, admin } = require('../config/kafka');

// Define the Avro schema
const valueSchema = avro.parse({
    name: 'EnergyData',
    type: 'record',
    fields: [
        { name: 'HourDK', type: 'string' },
        { name: 'HourUTC', type: 'string' },
        { name: 'PriceArea', type: 'string' },
        { name: 'SpotPriceDKK', type: 'float' },
        { name: 'SpotPriceEUR', type: 'float' },
    ],
});

let messages = [];

const initKafkaConsumer = async () => {
    try {
        await admin.connect();
        await consumer.connect();
        await consumer.subscribe({ topic: 'ELECTRICITY_PRICE', fromBeginning: false });

        console.log('Kafka consumer connected and subscribed to topic: ELECTRICITY_PRICE');

        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    let rawValue = message.value;

                    // If the message is Avro with schema registry, strip the 5-byte schema ID prefix
                    if (rawValue[0] === 0) {
                        rawValue = rawValue.subarray(5);
                    }

                    // Decode the Avro message
                    let decodedMessage;
                    try {
                        decodedMessage = valueSchema.fromBuffer(rawValue);
                        console.log(`Decoded Avro message: ${JSON.stringify(decodedMessage)}`);
                    } catch (avroError) {
                        console.warn('Failed to decode as Avro, trying JSON...');

                        // Attempt JSON parsing if Avro decoding fails
                        try {
                            const rawString = rawValue.toString(); // Convert buffer to string
                            decodedMessage = JSON.parse(rawString);
                            console.log(`Decoded JSON message: ${JSON.stringify(decodedMessage)}`);
                        } catch (jsonError) {
                            console.error('Failed to decode message as both Avro and JSON.', {
                                avroError,
                                jsonError,
                            });
                            return; // Skip this message if decoding fails
                        }
                    }

                    // Store the successfully decoded message
                    messages.push(decodedMessage);
                    if (messages.length > 100) {
                        messages.shift(); // Keep only the last 100 messages
                    }
                } catch (error) {
                    console.error('Unexpected error in Kafka consumer:', error);
                }
            },
        });

        // Adjust offsets and seek after the consumer starts running
        const offsets = await admin.fetchOffsets({ groupId: 'webapp-consumer-group', topic: 'ELECTRICITY_PRICE' });
        offsets.forEach(({ partition, offset }) => {
            const seekOffset = Math.max(0, parseInt(offset, 10) - 100).toString(); // Adjust for the last 100 messages
            consumer.seek({ topic: 'ELECTRICITY_PRICE', partition: 0, offset: seekOffset });
        });

    } catch (error) {
        console.error('Error initializing Kafka consumer', error);
    }
};

const getMessages = () => {
    return messages;
};

module.exports = { initKafkaConsumer, getMessages };
