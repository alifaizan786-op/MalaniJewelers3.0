// src/shared/rateLimiter.js
class RateLimiter {
  constructor(options = {}) {
    this.restCallsPerSecond = options.restCallsPerSecond || 1.5; // Conservative REST API limit
    this.graphqlPointsPerMinute =
      options.graphqlPointsPerMinute || 800; // Conservative GraphQL limit
    this.maxRetries = options.maxRetries || 5;
    this.baseDelay = options.baseDelay || 1000;

    // Track API usage
    this.restCallQueue = [];
    this.graphqlPointsUsed = 0;
    this.lastGraphqlReset = Date.now();

    // Queue for pending operations
    this.operationQueue = [];
    this.processing = false;
  }

  // Add operation to queue
  addOperation(operation) {
    return new Promise((resolve, reject) => {
      this.operationQueue.push({
        operation,
        resolve,
        reject,
        retries: 0,
      });

      if (!this.processing) {
        this.processQueue();
      }
    });
  }

  // Process operations with rate limiting
  async processQueue() {
    if (this.processing || this.operationQueue.length === 0) return;

    this.processing = true;

    while (this.operationQueue.length > 0) {
      const { operation, resolve, reject, retries } =
        this.operationQueue.shift();

      try {
        // Wait for rate limit if needed
        await this.waitForRateLimit(
          operation.type,
          operation.points || 1
        );

        // Execute operation
        const result = await operation.execute();

        // Track API usage
        this.trackApiUsage(operation.type, operation.points || 1);

        resolve(result);

        // Small delay between operations
        await this.delay(100);
      } catch (error) {
        if (this.shouldRetry(error, retries)) {
          // Re-queue with exponential backoff
          const delay = this.baseDelay * Math.pow(2, retries);
          await this.delay(delay);

          this.operationQueue.unshift({
            operation,
            resolve,
            reject,
            retries: retries + 1,
          });
        } else {
          reject(error);
        }
      }
    }

    this.processing = false;
  }

  // Check if we should retry the operation
  shouldRetry(error, retries) {
    if (retries >= this.maxRetries) return false;

    // Retry on rate limit errors
    if (error.response?.status === 429) return true;

    // Retry on temporary server errors
    if (error.response?.status >= 500) return true;

    // Retry on network errors
    if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT')
      return true;

    return false;
  }

  // Wait for rate limit availability
  async waitForRateLimit(type, points = 1) {
    if (type === 'rest') {
      // REST API: Clean old calls and check limit
      const now = Date.now();
      this.restCallQueue = this.restCallQueue.filter(
        (time) => now - time < 1000
      );

      if (this.restCallQueue.length >= this.restCallsPerSecond) {
        const oldestCall = Math.min(...this.restCallQueue);
        const waitTime = 1000 - (now - oldestCall) + 100; // Add 100ms buffer
        if (waitTime > 0) {
          await this.delay(waitTime);
        }
      }
    } else if (type === 'graphql') {
      // GraphQL: Reset points counter every minute
      const now = Date.now();
      if (now - this.lastGraphqlReset > 60000) {
        this.graphqlPointsUsed = 0;
        this.lastGraphqlReset = now;
      }

      // Wait if we would exceed the limit
      if (
        this.graphqlPointsUsed + points >
        this.graphqlPointsPerMinute
      ) {
        const waitTime = 60000 - (now - this.lastGraphqlReset) + 1000; // Add 1s buffer
        await this.delay(waitTime);
        this.graphqlPointsUsed = 0;
        this.lastGraphqlReset = Date.now();
      }
    }
  }

  // Track API usage
  trackApiUsage(type, points = 1) {
    if (type === 'rest') {
      this.restCallQueue.push(Date.now());
    } else if (type === 'graphql') {
      this.graphqlPointsUsed += points;
    }
  }

  // Utility delay function
  delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  // Get current rate limit status
  getStatus() {
    const now = Date.now();
    const recentRestCalls = this.restCallQueue.filter(
      (time) => now - time < 1000
    ).length;

    return {
      restCallsInLastSecond: recentRestCalls,
      restLimit: this.restCallsPerSecond,
      graphqlPointsUsed: this.graphqlPointsUsed,
      graphqlLimit: this.graphqlPointsPerMinute,
      queueLength: this.operationQueue.length,
      timeSinceGraphqlReset: now - this.lastGraphqlReset,
    };
  }
}

module.exports = RateLimiter;
