const { getMessages } = require('../services/kafkaService');

const getAllMessages = (req, res) => {
    const messages = getMessages();
    console.log(messages);
    res.render('home', { messages });
};

module.exports = { getAllMessages };