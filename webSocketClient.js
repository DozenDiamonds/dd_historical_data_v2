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

  // SESSION CREATION (correct)
  async createSession() {
    try {
      const smart = new SmartAPI({ api_key: this.apiKey });

      const session = await smart.generateSession(
        this.clientCode,
        this.password,
        this.totp
      );
      
      console.log('session', session)
      this.jwtToken = session.data.jwtToken;
      this.feedToken = session.data.feedToken; 
      // console.log("here is  the session  data  ", this.jwtToken)
      this.log("Session created.");

    } catch (err) {
      this.log(`Session error: ${err.message}`);
      throw err;
    }
  }

  // WEBSOCKET CONNECTION (correct)
  async connect() {
    await this.createSession();

   
    this.webSocket = new WebSocketV2({
      jwttoken: this.jwtToken,
      apikey: this.apiKey,
      clientcode: this.clientCode,
      feedtype: this.feedToken,
    });

    try {
      this.log("Connecting WebSocket...");
      await this.webSocket.connect();

      this.connected = true;
      this.retries = 0;

      this.log("WebSocket connected.");

      this.webSocket.on("tick", (data) => this.onTick(data)); 
      this.webSocket.on("close", () => this.onClose());
      this.webSocket.on("error", (err) => this.onError(err)); 

     // this.heartbeat(); ->websocket  internally handle the  ping  
      return this.webSocket; 

    } catch (err) {
      this.log(`WebSocket connection failed: ${err.message}`);
      this.connected = false;
      this.attemptReconnect();
    }
  }

  onTick(data) {
    if (data === "pong") return;
    this.log(`Tick: ${JSON.stringify(data)}`);
  }

  onClose() {
    this.log("Closed. Reconnecting...");
    this.connected = false;
    this.attemptReconnect();
  }

  onError(err) {
    this.log(`Error: ${err.message}`);
    this.connected = false;
    this.attemptReconnect();
  }

  attemptReconnect() {
    if (this.retries >= this.maxRetries) {
      this.log("Max retries reached.");
      return;
    }

    this.retries++;
    const delay = this.baseRetryDelay * 2 ** this.retries;

    this.log(`Retry in ${delay / 1000}s`);

    setTimeout(() => this.connect(), delay);
  }

  // heartbeat() {
  //   setInterval(() => {
  //     if (this.connected && this.webSocket) {
  //       try {
  //         this.webSocket.send("ping");
  //       } catch (err) {
  //         this.log(`Ping failed: ${err.message}`);
  //       }
  //     }
  //   }, 15000);
  // }

  async terminate() {
    if (this.webSocket) {
      this.log("Closing websocket...");
      this.webSocket.close();
    }
    this.connected = false;
  }

  static async create(config) {
    const client = new WebSocketClient(config);
    await client.connect();
    return client;
  }
}

module.exports = WebSocketClient;
