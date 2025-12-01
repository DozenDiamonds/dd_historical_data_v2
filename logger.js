// logger.js - Modified for multiple instances
const winston = require("winston");

class Logger {
  constructor() {
    if (Logger.instance) {
      return Logger.instance;
    }
    
    this.initializeLogger();
    Logger.instance = this;
  }

  initializeLogger() {
    const logFormat = winston.format.combine(
      winston.format.timestamp({
        format: "YYYY-MM-DD HH:mm:ss"
      }),
      winston.format.errors({ stack: true }),
      winston.format.json()
    );

    this.logger = winston.createLogger({
      level: "info",
      format: logFormat,
      defaultMeta: { service: "websocket-cluster" },
      transports: [
        new winston.transports.File({ 
          filename: "logs/websocket-error.log", 
          level: "error",
          maxsize: 10485760, // 10MB
          maxFiles: 10
        }),
        new winston.transports.File({ 
          filename: "logs/websocket-combined.log",
          maxsize: 10485760, // 10MB
          maxFiles: 10
        }),
        new winston.transports.Console({
          format: winston.format.combine(
            winston.format.colorize(),
            winston.format.simple()
          )
        })
      ]
    });
  }

  info(message, instance = "default") {
    this.logger.info(message, { instance });
  }

  warn(message, instance = "default") {
    this.logger.warn(message, { instance });
  }

  error(message, instance = "default") {
    this.logger.error(message, { instance });
  }
}

module.exports = Logger;