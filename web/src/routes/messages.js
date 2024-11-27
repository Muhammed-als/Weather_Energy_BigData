const express = require('express');
const router = express.Router();
const { getAllMessages } = require('../controllers/kafkaController');

router.get('/', getAllMessages);

module.exports = router;