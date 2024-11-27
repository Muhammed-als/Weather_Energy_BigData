const avro = require('avro-js');
const { consumer } = require('../config/kafka');

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
        await consumer.connect();
        await consumer.subscribe({ topic: 'ENERGY_DATA_PARQUET', fromBeginning: true });

        console.log('Kafka consumer connected and subscribed to topic: ENERGY_DATA_PARQUET');
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                try {
                    let rawValue = message.value;

                    // If the message is Avro with schema registry, strip the 5-byte schema ID prefix
                    if (rawValue[0] === 0) {
                        // Extract payload by removing the first 5 bytes (schema ID)
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
                    if (messages.length > 20) {
                        messages.shift(); // Keep only the last 20 messages
                    }
                } catch (error) {
                    console.error('Unexpected error in Kafka consumer:', error);
                }
            },
        });
    } catch (error) {
        console.error('Error initializing Kafka consumer', error);
    }
};

const getMessages = () => {
    return messages;
};

module.exports = { initKafkaConsumer, getMessages };
