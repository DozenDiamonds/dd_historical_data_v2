/**
 * streamWriter.js 
 * (ok tested here) 
 *
 * A small production-ready StreamWriter class for appending CSV rows to a file
 * safely using Node.js WriteStream with backpressure handling and an internal
 * queue so multiple producers can call appendBatch concurrently.
 *
 * Usage (high level):
 *   const StreamWriter = require('./streamWriter');
 *   const writer = new StreamWriter('./output/data.csv', { highWaterMark: 16 * 1024 });
 *   await writer.appendBatch(["a,1,2\n", "b,3,4\n"]);
 *   await writer.close();
 *
 * Notes:
 * - This file intentionally focuses on safe, non-blocking append behavior.
 * - It exposes an async appendBatch API that resolves once the batch is flushed
 *   to the underlying stream (or at least queued safely).
 */

const fs = require('fs');
const { dirname } = require('path');
const { once } = require('events');

class StreamWriter {
  /**
   * @param {string} filePath - full path to file
   * @param {object} options
   *    - flags (default 'a')
   *    - encoding (default 'utf8')
   *    - highWaterMark (stream buffer size)
   */
  constructor(filePath, options = {}) {
    if (!filePath) throw new Error('filePath is required');

    this.filePath = filePath;
    this.options = Object.assign({ flags: 'a', encoding: 'utf8' }, options);

    // ensure directory exists (synchronous here is fine on startup)
    try {
      fs.mkdirSync(dirname(filePath), { recursive: true });
    } catch (err) {
      // ignore if exists
    }

    // Create a WriteStream in append mode
    this.stream = fs.createWriteStream(this.filePath, this.options);

    // Internal queue to hold batches: each item is { rows: string[], resolve, reject }
    this.queue = [];
    this.processing = false; // whether a worker is processing queue
    this.destroyed = false;

    // Listen for stream errors
    this.stream.on('error', (err) => {
      // propagate error to any current queue items
      const q = this.queue.splice(0);
      q.forEach(item => item.reject(err));
    });
  }

  /**
   * Append an array of CSV row strings to the file.
   * Each row should include its terminating newline ("\n").
   * Returns a Promise that resolves once the batch is flushed (or queued).
   *
   * @param {string[]} rows
   * @returns {Promise<void>}
   */
  appendBatch(rows) {
    if (!Array.isArray(rows)) throw new TypeError('rows must be an array of strings');
    if (this.destroyed) return Promise.reject(new Error('StreamWriter is closed'));

    return new Promise((resolve, reject) => {
      this.queue.push({ rows, resolve, reject });
      // Start processing if not already
      if (!this.processing) this._processQueue().catch(err => {
        // ensure unhandled errors are passed to promises
        console.error('StreamWriter processing error', err);
      });
    });
  }

  /**
   * Internal queue processor. It will take batches sequentially and write
   * them to the stream while respecting backpressure (waiting for 'drain').
   */
  async _processQueue() {
    this.processing = true;
    while (this.queue.length > 0) {
      const item = this.queue.shift();
      try {
        await this._writeRows(item.rows);
        item.resolve();
      } catch (err) {
        item.reject(err);
      }
    }
    this.processing = false;
  }

  /**
   * Low-level writer: writes multiple rows and waits for drain if needed.
   *
   * @param {string[]} rows
   */
  async _writeRows(rows) {
    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      // stream.write returns false when internal buffer is full
      const ok = this.stream.write(row);
      if (!ok) {
        // wait for 'drain' event before continuing
        await once(this.stream, 'drain');
      }
    }
  }

  /**
   * Gracefully close the writer. Ensures queued items are flushed first.
   * Returns a Promise that resolves when stream finishes.
   */
  async close() {
    if (this.destroyed) return;
    // wait for queue to finish
    while (this.processing || this.queue.length > 0) {
      // small delay - yields to event loop
      await new Promise(res => setImmediate(res));
    }
    // end the stream and wait for 'finish'
    this.stream.end();
    await once(this.stream, 'finish');
    this.destroyed = true;
  }
}

module.exports = StreamWriter;
