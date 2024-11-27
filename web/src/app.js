const express = require('express');
const path = require('path');
const routes = require('./routes');
const { initKafkaConsumer } = require('./services/kafkaService');

const app = express();
const PORT = process.env.PORT || 1337;

// Middleware for serving static files
app.use(express.static(path.join(__dirname, '/public')));

// Middleware to parse JSON
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

// Set view engine
app.set('views', path.join(__dirname, '/views'));
app.set('view engine', 'ejs');

// Use routes
app.use('/', routes);

// Start Kafka consumer
//initKafkaConsumer().catch((err) => {
//  console.error('Failed to start Kafka consumer:', err);
//});

// Start the server
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});