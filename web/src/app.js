const express = require('express');
const cors = require('cors');
const config = require('./config/config');
const path = require('path');
const router = express.Router();

const app = express();

app.use(express.static(__dirname + '/public'));

// Templating
app.set('view engine', 'ejs');
app.set("views", path.join(__dirname, "views"));

// Middleware
app.use(cors());
app.use(express.json());

// Routes
router.use((req, res, next) => {
    next();
});

router.get('/', (req, res, next) => {
    res.render('home');
});

app.use('/', router);


// Start the server
app.listen(config.port, () => {
  console.log(`Server is running on port ${config.port}`);
});