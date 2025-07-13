const express = require('express');

function createUserRoutes(userController) {
  const router = express.Router();

  // Test route
  router.get('/test', (req, res) => {
    res.json({ message: 'Test route working' });
  });

  // Simple POST test
  router.post('/test', (req, res) => {
    res.json({ message: 'POST test working', body: req.body });
  });

  router.post('/users', (req, res, next) => {
    console.log('POST /users route hit');
    userController.createUser(req, res, next);
  });
  
  router.get('/users', (req, res, next) => {
    console.log('GET /users route hit');
    userController.getAllUsers(req, res, next);
  });
  
  router.get('/users/:id', (req, res, next) => {
    console.log('GET /users/:id route hit');
    userController.getUser(req, res, next);
  });
  
  router.put('/users/:id', (req, res, next) => {
    console.log('PUT /users/:id route hit');
    userController.updateUser(req, res, next);
  });
  
  router.delete('/users/:id', (req, res, next) => {
    console.log('DELETE /users/:id route hit');
    userController.deleteUser(req, res, next);
  });

  return router;
}

module.exports = createUserRoutes;