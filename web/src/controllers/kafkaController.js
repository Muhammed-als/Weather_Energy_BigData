const { getMessages } = require('../services/kafkaService');

const getAllMessages = (req, res) => {
    const messages = getMessages();

    res.render('home', { messages });
};

module.exports = { getAllMessages };