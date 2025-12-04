const fs = require("fs");
const path = require("path");
const { SmartAPI, WebSocketV2 } = require("smartapi-javascript");

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
    this.isConnecting = false;

    this.retries = 0;
    this.maxRetries = 10;
    this.baseRetryDelay = 1000;

    this.logFile = path.join(__dirname, `${this.name}.log`);
  }

  log(msg) {
    const line = `[${new Date().toISOString()}] [${this.name}] ${msg}`;
    console.log(line);
    fs.appendFileSync(this.logFile, line + "\n");
  }

  async createSession() {
    try {
      const smart = new SmartAPI({ api_key: this.apiKey });
      const session = await smart.generateSession(
        this.clientCode,
        this.password,
        this.totp
      );
      
      this.jwtToken = session.data.jwtToken;
      this.feedToken = session.data.feedToken; 
      this.log("Session created.");
    } catch (err) {
      this.log(`Session error: ${err.message}`);
      throw err;
    }
  }

  // SIMPLE CONNECTION WITH TIMEOUT - No hanging!
  async connect() {
    // Prevent multiple simultaneous connection attempts
    if (this.isConnecting) {
      this.log("Already connecting, skipping...");
      return;
    }
    
    this.isConnecting = true;
    
    try {
      await this.createSession();

      this.webSocket = new WebSocketV2({
        jwttoken: this.jwtToken,
        apikey: this.apiKey,
        clientcode: this.clientCode,
        feedtype: this.feedToken,
      });

      this.log("Connecting WebSocket...");
      
      // IMPORTANT: Add timeout to prevent hanging
      const connectWithTimeout = () => {
        return new Promise((resolve, reject) => {
          const timeout = setTimeout(() => {
            reject(new Error("WebSocket connection timeout (10s)"));
          }, 10000); // 10 second timeout

          this.webSocket.connect()
            .then(() => {
              clearTimeout(timeout);
              resolve();
            })
            .catch(err => {
              clearTimeout(timeout);
              reject(err);
            });
        });
      };

      await connectWithTimeout();

      this.connected = true;
      this.retries = 0;
      this.isConnecting = false;

      this.log("WebSocket connected.");

      this.webSocket.on("tick", (data) => this.onTick(data)); 
      this.webSocket.on("close", () => this.onClose());
      this.webSocket.on("error", (err) => this.onError(err));

      return this.webSocket;

    } catch (err) {
      this.isConnecting = false;
      this.connected = false;
      this.log(`WebSocket connection failed: ${err.message}`);
      
      // Simple reconnect attempt
      this.simpleReconnect();
      
      // Re-throw so caller knows connection failed
      throw err;
    }
  }

  onTick(data) {
    if (data === "pong") return;
    this.log(`Tick: ${JSON.stringify(data)}`);
  }

  onClose() {
    this.log("Closed. Reconnecting...");
    this.connected = false;
    this.simpleReconnect();
  }

  onError(err) {
    this.log(`Error: ${err.message}`);
    this.connected = false;
    this.simpleReconnect();
  }

  // SIMPLE RECONNECT - No complex logic
  simpleReconnect() {
    if (this.retries >= this.maxRetries) {
      this.log("Max retries reached.");
      return;
    }

    this.retries++;
    const delay = Math.min(this.baseRetryDelay * 2 ** this.retries, 30000); // Max 30s

    this.log(`Retry ${this.retries}/${this.maxRetries} in ${delay / 1000}s`);

    setTimeout(() => {
      // Don't reconnect if we're already trying
      if (!this.isConnecting && !this.connected) {
        this.connect().catch(() => {
          // Errors already logged in connect()
        });
      }
    }, delay);
  }

  isAlive() {
    try {
      return this.webSocket && this.webSocket._ws && this.webSocket._ws.readyState === 1;
    } catch {
      return false;
    }
  }

  async terminate() {
    // Set a flag to prevent reconnects during termination
    this.isConnecting = false;
    
    if (this.webSocket) {
      this.log("Closing websocket...");
      this.webSocket.close();
    }
    this.connected = false;
  }

  static async create(config) {
    const client = new WebSocketClient(config);
    try {
      await client.connect();
      return client;
    } catch (error) {
      // Log but don't throw - client will keep trying to reconnect
      client.log(`Initial connection failed, will retry: ${error.message}`);
      return client; // Still return client, it will reconnect automatically
    }
  }
}

module.exports = WebSocketClient;