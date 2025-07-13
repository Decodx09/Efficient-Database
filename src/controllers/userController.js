const UserModel = require('../models/userModel');
const kafkaProducer = require('../services/kafkaProducer');
const logger = require('../utils/logger');

class UserController {
  constructor(dbConnections) {
    console.log('UserController constructor called');
    console.log('dbConnections keys:', Object.keys(dbConnections));
    this.userModel = new UserModel(dbConnections);
    console.log('UserController initialized');
  }

  async createUser(req, res, next) {
    console.log('=== createUser method called ===');
    console.log('Request body:', req.body);
    
    try {
      const { name, email } = req.body;
      console.log('Extracted name:', name, 'email:', email);

      if (!name || !email) {
        console.log('Validation failed - missing name or email');
        return res.status(400).json({
          success: false,
          error: 'Name and email are required'
        });
      }

      console.log('About to call userModel.create');
      const user = await this.userModel.create({ name, email });
      console.log('User created successfully:', user);

      res.status(201).json({
        success: true,
        data: user
      });
    } catch (error) {
      console.error('=== Error in createUser ===');
      console.error('Error message:', error.message);
      console.error('Error stack:', error.stack);
      next(error);
    }
  }

  async getAllUsers(req, res, next) {
    try {
      const users = await this.userModel.findAll();
      
      res.json({
        success: true,
        data: users,
        count: users.length
      });
    } catch (error) {
      next(error);
    }
  }

  async getUser(req, res, next) {
    try {
      const id = parseInt(req.params.id);
      const user = await this.userModel.findById(id);

      if (!user) {
        return res.status(404).json({
          success: false,
          error: 'User not found'
        });
      }

      res.json({
        success: true,
        data: user
      });
    } catch (error) {
      next(error);
    }
  }

  async updateUser(req, res, next) {
    try {
      const id = parseInt(req.params.id);
      const updated = await this.userModel.update(id, req.body);

      if (!updated) {
        return res.status(404).json({
          success: false,
          error: 'User not found'
        });
      }

      res.json({
        success: true,
        message: 'User updated successfully'
      });
    } catch (error) {
      next(error);
    }
  }

  async deleteUser(req, res, next) {
    try {
      const id = parseInt(req.params.id);
      const deleted = await this.userModel.delete(id);

      if (!deleted) {
        return res.status(404).json({
          success: false,
          error: 'User not found'
        });
      }

      res.json({
        success: true,
        message: 'User deleted successfully'
      });
    } catch (error) {
      next(error);
    }
  }
}

module.exports = UserController;