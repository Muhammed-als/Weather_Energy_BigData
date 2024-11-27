const express = require('express');
const router = express.Router();
const messageRoutes = require('./messages');

// Use message routes
router.use('/', messageRoutes);

module.exports = router;