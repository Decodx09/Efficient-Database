const express = require('express');
const createUserRoutes = require('./routes/userRoutes');
const errorHandler = require('./middleware/errorHandler');
const UserController = require('./controllers/userController');

function createApp(dbConnections) {
  const app = express();

  // Debug middleware
  app.use((req, res, next) => {
    console.log(`${req.method} ${req.path}`, req.body);
    next();
  });

  // Middleware
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));

  // Health check
  app.get('/health', (req, res) => {
    res.json({ status: 'OK', timestamp: new Date().toISOString() });
  });

  // Routes
  const userController = new UserController(dbConnections);
  app.use('/api', createUserRoutes(userController));

  // Error handling
  app.use(errorHandler);

  return app;
}

module.exports = createApp;