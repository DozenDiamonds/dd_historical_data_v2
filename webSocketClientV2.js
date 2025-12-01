const fs = require("fs");
const path = require("path");
const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");
const Logger = require("./logger");

class WebSocketClient {
  constructor({ apiKey, clientCode, password, totp, name = "WebSocket" }) {
    this.apiKey = apiKey;
    this.clientCode = clientCode;
    this.password = password;
    this.totp = totp;
    this.name = name;

    this.jwtToken = null;
    this.webSocket = null;
    this.connected = false;

    this.retries = 0;
    this.maxRetries = 10;
    this.baseRetryDelay = 1000;

    this.sessionRetries = 0;
    this.maxSessionRetries = 3;
    this.sessionRetryDelay = 2000;

    this.reconnectTimeout = null;
    this.isReconnecting = false;

    // Option 2: Track if listeners have been added
    this.listenersAdded = false;

    this.logger = new Logger(this.name);
  }

  async createSession() {
    while (this.sessionRetries < this.maxSessionRetries) {
      try {
        const smart = new SmartAPI({ api_key: this.apiKey });
        const session = await smart.generateSession(
          this.clientCode,
          this.password,
          this.totp
        );

        this.jwtToken = session.data.jwtToken;
        this.feedToken = session.data.feedToken;
        this.sessionRetries = 0;
        this.logger.info("Session created successfully.");
        return;

      } catch (err) {
        this.sessionRetries++;
        this.logger.error(
          `Session creation failed (${this.sessionRetries}/${this.maxSessionRetries}): ${err.message}`
        );

        if (this.sessionRetries >= this.maxSessionRetries) throw err;
        await this.delay(this.sessionRetryDelay);
      }
    }
  }

  delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /** 
   * Main Connection 
   */
  async connect() {
    try {
      await this.createSession();

      // Create a FRESH WebSocketV2 object every time
      this.webSocket = new WebSocketV2({
        jwttoken: this.jwtToken,
        apikey: this.apiKey,
        clientcode: this.clientCode,
        feedtype: this.feedToken,
      });

      this.logger.info("Connecting WebSocket...");

      await this.webSocket.connect();

      this.connected = true;
      this.retries = 0;
      this.sessionRetries = 0;
      this.isReconnecting = false;

      this.logger.info("WebSocket connected.");

      // Add listeners only ONCE per instance
      if (!this.listenersAdded) {
        this.webSocket.on("tick", (d) => this.onTick(d));
        this.webSocket.on("close", () => this.onClose());
        this.webSocket.on("error", (err) => this.onError(err));

        this.listenersAdded = true;
      }

      return this.webSocket;

    } catch (err) {
      this.logger.error(`WebSocket connection failed: ${err.message}`);
      this.connected = false;
      this.attemptReconnect();
      throw err;
    }
  }

  onTick(data) {
    if (data === "pong") return;
    this.logger.info(`Tick: ${JSON.stringify(data)}`);
  }

  onClose() {
    this.logger.warn("WebSocket closed. Reconnecting...");
    this.connected = false;
    this.attemptReconnect();
  }

  onError(err) {
    this.logger.error(`WebSocket error: ${err.message}`);
    this.connected = false;
    this.attemptReconnect();
  }

  /**
   * Reconnection Logic
   */
  attemptReconnect() {
    if (this.isReconnecting) return;

    if (this.retries >= this.maxRetries) {
      this.logger.error("Max reconnection retries reached.");
      return;
    }

    this.isReconnecting = true;
    this.retries++;

    const delay = Math.min(
      this.baseRetryDelay * Math.pow(2, this.retries),
      30000
    );

    this.logger.warn(
      `Reconnecting ${this.retries}/${this.maxRetries} in ${delay / 1000}s...`
    );

    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
    }

    this.reconnectTimeout = setTimeout(async () => {
      try {
        await this.connect();
      } catch (err) {
        this.isReconnecting = false;
      }
    }, delay);
  }

  isTokenValid() {
    if (!this.jwtToken) return false;

    try {
      const payload = JSON.parse(
        Buffer.from(this.jwtToken.split(".")[1], "base64").toString()
      );
      return payload.exp > Date.now() / 1000 + 60;

    } catch (err) {
      this.logger.warn("Token parsing failed: " + err.message);
      return false;
    }
  }

  /**
   * Proper Cleanup
   */
  async terminate() {
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    this.isReconnecting = false;
    this.connected = false;

    if (this.webSocket) {
      try {
        this.logger.info("Closing WebSocket...");
        this.webSocket.close();
      } catch (err) {
        this.logger.error("Error closing WebSocket: " + err.message);
      }
      this.webSocket = null;
    }

    // Do NOT reset listenersAdded, to avoid re-binding duplicates
    this.retries = 0;
    this.sessionRetries = 0;
  }

  static async create(config) {
    const client = new WebSocketClient(config);
    await client.connect();
    return client;
  }
}

module.exports = WebSocketClient;
