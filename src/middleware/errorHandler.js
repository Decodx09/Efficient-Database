const logger = require('../utils/logger');

function errorHandler(err, req, res, next) {
  logger.error('Error:', err);

  if (err.code === 'ER_DUP_ENTRY') {
    return res.status(409).json({
      success: false,
      error: 'Email already exists'
    });
  }

  res.status(500).json({
    success: false,
    error: 'Internal server error'
  });
}

module.exports = errorHandler;