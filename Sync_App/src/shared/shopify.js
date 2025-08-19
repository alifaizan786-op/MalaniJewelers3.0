// src/shared/shopify.js
// src/shared/shopify.js
const axios = require('axios');
const RateLimiter = require('./rateLimiter');
require('dotenv').config();

const SHOP = process.env.Shopify_Shop_Name.replace(/['"]+/g, '');
const ACCESS_TOKEN = process.env.Shopify_Admin_Api_Access_Token;

// Store-specific rate limiters to prevent conflicts between concurrent syncs
const rateLimiters = new Map();

// Get or create rate limiter for specific store
function getRateLimiterForStore(storeCode) {
  if (!rateLimiters.has(storeCode)) {
    rateLimiters.set(
      storeCode,
      new RateLimiter({
        restCallsPerSecond: 1.0, // More conservative for multiple stores
        graphqlPointsPerMinute: 600, // More conservative for multiple stores
        maxRetries: 3,
        baseDelay: 2000,
      })
    );
  }
  return rateLimiters.get(storeCode);
}

// Enhanced error handling for Shopify API responses
function handleShopifyError(error, context) {
  const response = error.response;
  if (response) {
    const status = response.status;
    const data = response.data;

    // Better error message extraction
    let errorMessage = 'Unknown error';
    if (data) {
      if (typeof data === 'string') {
        errorMessage = data;
      } else if (data.errors) {
        if (Array.isArray(data.errors)) {
          errorMessage = data.errors.join(', ');
        } else if (typeof data.errors === 'object') {
          errorMessage = JSON.stringify(data.errors);
        } else {
          errorMessage = String(data.errors);
        }
      } else if (data.error) {
        errorMessage = String(data.error);
      } else if (data.message) {
        errorMessage = String(data.message);
      } else {
        errorMessage = JSON.stringify(data);
      }
    }

    if (status === 429) {
      console.warn(`⚠️ Rate limit hit in ${context}:`, data);
      throw new Error(`Rate limit exceeded: ${errorMessage}`);
    }

    if (status >= 400 && status < 500) {
      console.error(`❌ Client error in ${context}:`, data);
      throw new Error(`API error (${status}): ${errorMessage}`);
    }

    if (status >= 500) {
      console.error(`❌ Server error in ${context}:`, data);
      throw new Error(`Server error (${status}): ${errorMessage}`);
    }
  }

  // Handle network errors
  if (error.code) {
    throw new Error(
      `Network error: ${error.code} - ${error.message}`
    );
  }

  throw new Error(
    `Unknown error in ${context}: ${error.message || error}`
  );
}

// Chunk array into smaller batches
function chunkArray(array, chunkSize) {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

// Batched SKU lookup via GraphQL with chunking and rate limiting
async function getInventoryItemIdsBySKUs(skuList, logger, storeCode) {
  if (!skuList || skuList.length === 0) return {};

  const rateLimiter = getRateLimiterForStore(storeCode);

  logger(
    storeCode,
    `🔍 Looking up ${skuList.length} SKUs in Shopify...`
  );

  // Chunk SKUs to avoid query size limits and improve rate limiting
  const chunks = chunkArray(skuList, 30); // Smaller chunks for better stability with multiple stores
  const allResults = {};

  for (let i = 0; i < chunks.length; i++) {
    const chunk = chunks[i];
    logger(
      storeCode,
      `📦 Processing SKU batch ${i + 1}/${chunks.length} (${
        chunk.length
      } SKUs)`
    );

    const operation = {
      type: 'graphql',
      points: 8, // More conservative estimate
      execute: async () => {
        const queryString = chunk
          .map((sku) => `sku:${sku}`)
          .join(' OR ');
        const query = `
          query {
            productVariants(first: ${chunk.length}, query: "${queryString}") {
              edges {
                node {
                  id
                  sku
                  inventoryItem {
                    id
                  }
                }
              }
            }
          }
        `;

        try {
          const response = await axios.post(
            `https://${SHOP}.myshopify.com/admin/api/2024-04/graphql.json`,
            { query },
            {
              headers: {
                'X-Shopify-Access-Token': ACCESS_TOKEN,
                'Content-Type': 'application/json',
              },
              timeout: 30000, // 30 second timeout
            }
          );

          return response.data;
        } catch (error) {
          // Log the full error for debugging
          console.error(
            `GraphQL error for store ${storeCode}, batch ${i + 1}:`,
            error.response?.data || error.message
          );
          throw error;
        }
      },
    };

    try {
      const result = await rateLimiter.addOperation(operation);

      if (result.errors) {
        logger(
          storeCode,
          `⚠️ GraphQL errors in batch ${i + 1}: ${JSON.stringify(
            result.errors
          )}`
        );
        continue;
      }

      const variants = result.data.productVariants.edges;

      for (const v of variants) {
        const inventoryItemIdGlobal = v.node.inventoryItem.id;
        const inventoryItemId = inventoryItemIdGlobal
          .split('/')
          .pop();
        allResults[v.node.sku] = inventoryItemId;
      }

      logger(
        storeCode,
        `✅ Batch ${i + 1}/${chunks.length}: Found ${
          Object.keys(allResults).length
        } total SKUs so far`
      );
    } catch (error) {
      handleShopifyError(
        error,
        `SKU lookup batch ${i + 1} for store ${storeCode}`
      );
      logger(
        storeCode,
        `❌ Failed to process SKU batch ${i + 1}: ${error.message}`
      );
      // Don't throw here, continue with remaining batches
    }
  }

  logger(
    storeCode,
    `🎯 Final result: Found ${Object.keys(allResults).length}/${
      skuList.length
    } SKUs in Shopify`
  );
  return allResults;
}

// Rate-limited inventory update
async function updateInventoryLevel(
  inventoryItemId,
  quantity,
  locationId,
  logger,
  storeCode,
  sku
) {
  const rateLimiter = getRateLimiterForStore(storeCode);

  const operation = {
    type: 'rest',
    execute: async () => {
      try {
        const response = await axios.post(
          `https://${SHOP}.myshopify.com/admin/api/2024-04/inventory_levels/set.json`,
          {
            location_id: locationId,
            inventory_item_id: inventoryItemId,
            available: quantity,
          },
          {
            headers: {
              'X-Shopify-Access-Token': ACCESS_TOKEN,
            },
            timeout: 15000, // 15 second timeout
          }
        );

        return response.data;
      } catch (error) {
        // Log the full error for debugging
        console.error(
          `Inventory update error for store ${storeCode}, SKU ${sku}:`,
          error.response?.data || error.message
        );
        throw error;
      }
    },
  };

  try {
    await rateLimiter.addOperation(operation);
    logger(
      storeCode,
      `📦 Updated inventory for SKU ${sku}: ${quantity} units`
    );
    return true;
  } catch (error) {
    handleShopifyError(
      error,
      `inventory update for SKU ${sku} in store ${storeCode}`
    );
    logger(
      storeCode,
      `❌ Failed to update inventory for SKU ${sku}: ${error.message}`
    );
    return false;
  }
}

// Rate-limited product evaluation with batching
async function evaluateProductForDrafting(
  inventoryItemId,
  locationId,
  storeCode,
  logger
) {
  const rateLimiter = getRateLimiterForStore(storeCode);

  const operation1 = {
    type: 'graphql',
    points: 12, // Conservative estimate for this complex query
    execute: async () => {
      const queryVariant = `
        query {
          inventoryItem(id: "gid://shopify/InventoryItem/${inventoryItemId}") {
            variant {
              id
              product {
                id
                handle
                title
                status
                collections(first: 10) {
                  edges {
                    node {
                      handle
                      title
                    }
                  }
                }
                variants(first: 100) {
                  edges {
                    node {
                      inventoryItem {
                        id
                      }
                      inventoryQuantity
                      sku
                    }
                  }
                }
              }
            }
          }
        }
      `;

      try {
        const response = await axios.post(
          `https://${SHOP}.myshopify.com/admin/api/2024-04/graphql.json`,
          { query: queryVariant },
          {
            headers: {
              'X-Shopify-Access-Token': ACCESS_TOKEN,
              'Content-Type': 'application/json',
            },
            timeout: 30000,
          }
        );

        return response.data;
      } catch (error) {
        // Log the full error for debugging
        console.error(
          `Product evaluation GraphQL error for store ${storeCode}, inventory item ${inventoryItemId}:`,
          error.response?.data || error.message
        );
        throw error;
      }
    },
  };

  try {
    const result = await rateLimiter.addOperation(operation1);

    if (result.errors) {
      logger(
        storeCode,
        `⚠️ GraphQL errors in product evaluation: ${JSON.stringify(
          result.errors
        )}`
      );
      return false;
    }

    if (!result.data.inventoryItem?.variant?.product) {
      logger(
        storeCode,
        `⚠️ No product found for inventory item ${inventoryItemId}`
      );
      return false;
    }

    const productNode = result.data.inventoryItem.variant.product;
    const variants = productNode.variants.edges;
    const productId = productNode.id;
    const productHandle = productNode.handle;
    const currentStatus = productNode.status;

    // Skip if already drafted
    if (currentStatus === 'draft') {
      logger(
        storeCode,
        `📋 Product ${productHandle} is already drafted`
      );
      return false;
    }

    // Check if any sibling has inventory > 0
    const anyInStock = variants.some(
      (v) => v.node.inventoryQuantity > 0
    );

    if (anyInStock) {
      logger(
        storeCode,
        `📦 Product ${productHandle} still has variants in stock — no action needed`
      );
      return false;
    }

    logger(
      storeCode,
      `🎯 Product ${productHandle} is out of stock - proceeding to draft...`
    );

    // Set product to draft (REST API)
    const operation2 = {
      type: 'rest',
      execute: async () => {
        const productNumericId = productId.split('/').pop();

        try {
          const response = await axios.put(
            `https://${SHOP}.myshopify.com/admin/api/2024-04/products/${productNumericId}.json`,
            {
              product: {
                id: productNumericId,
                status: 'draft',
              },
            },
            {
              headers: {
                'X-Shopify-Access-Token': ACCESS_TOKEN,
              },
              timeout: 15000,
            }
          );

          return response.data;
        } catch (error) {
          // Log the full error for debugging
          console.error(
            `Product draft error for store ${storeCode}, product ${productHandle}:`,
            error.response?.data || error.message
          );
          throw error;
        }
      },
    };

    await rateLimiter.addOperation(operation2);
    logger(storeCode, `✅ Product ${productHandle} set to draft`);

    // Create redirect if product has collections
    const collections = productNode.collections.edges;
    if (collections.length === 0) {
      logger(
        storeCode,
        `📝 Product ${productHandle} has no collections, skipping redirect`
      );
      return true;
    }

    // Pick longest collection title (most specific)
    collections.sort(
      (a, b) => b.node.title.length - a.node.title.length
    );
    const targetCollection = collections[0].node.handle;

    const operation3 = {
      type: 'rest',
      execute: async () => {
        try {
          const response = await axios.post(
            `https://${SHOP}.myshopify.com/admin/api/2024-04/redirects.json`,
            {
              redirect: {
                path: `/${productHandle}`,
                target: `/collections/${targetCollection}`,
              },
            },
            {
              headers: {
                'X-Shopify-Access-Token': ACCESS_TOKEN,
              },
              timeout: 15000,
            }
          );

          return response.data;
        } catch (error) {
          // Log the full error for debugging but don't fail the whole operation
          console.error(
            `Redirect creation error for store ${storeCode}, product ${productHandle}:`,
            error.response?.data || error.message
          );

          // If it's a duplicate redirect error, that's OK
          if (
            error.response?.status === 422 &&
            error.response?.data?.errors?.path
          ) {
            logger(
              storeCode,
              `📝 Redirect already exists for ${productHandle}`
            );
            return { redirect: { path: `/${productHandle}` } }; // Return success-like response
          }

          throw error;
        }
      },
    };

    try {
      await rateLimiter.addOperation(operation3);
      logger(
        storeCode,
        `🔗 Redirect created: ${productHandle} → /collections/${targetCollection}`
      );
    } catch (error) {
      // Don't fail the whole operation if redirect creation fails
      logger(
        storeCode,
        `⚠️ Failed to create redirect for ${productHandle}: ${error.message}`
      );
    }

    return true;
  } catch (error) {
    handleShopifyError(
      error,
      `product evaluation for inventory item ${inventoryItemId} in store ${storeCode}`
    );
    logger(
      storeCode,
      `❌ Failed to evaluate product: ${error.message}`
    );
    return false;
  }
}

// Batch process inventory updates with progress tracking
async function batchUpdateInventory(
  records,
  locationId,
  storeCode,
  logger,
  progressCallback
) {
  const rateLimiter = getRateLimiterForStore(storeCode);

  logger(
    storeCode,
    `🚀 Starting batch inventory update for ${records.length} records`
  );

  // Step 1: Get all inventory item IDs in batches
  const skuList = records
    .map((r) => r.SKU_NO?.trim())
    .filter((sku) => sku);

  const skuMap = await getInventoryItemIdsBySKUs(
    skuList,
    logger,
    storeCode
  );

  // Step 2: Process inventory updates with rate limiting
  const inventoryUpdates = records
    .filter((rec) => {
      const sku = rec.SKU_NO?.trim();
      return sku && skuMap[sku];
    })
    .map((rec) => ({
      sku: rec.SKU_NO.trim(),
      qty: parseInt(rec.QTYONHAND || 0),
      inventoryItemId: skuMap[rec.SKU_NO.trim()],
    }));

  logger(
    storeCode,
    `📦 Processing ${inventoryUpdates.length} inventory updates...`
  );

  let successCount = 0;
  const total = inventoryUpdates.length;

  for (let i = 0; i < inventoryUpdates.length; i++) {
    const { sku, qty, inventoryItemId } = inventoryUpdates[i];

    // Update progress
    if (progressCallback) {
      progressCallback({
        current: i + 1,
        total,
        sku,
        phase: 'inventory_update',
      });
    }

    // Update inventory
    const updated = await updateInventoryLevel(
      inventoryItemId,
      qty,
      locationId,
      logger,
      storeCode,
      sku
    );

    if (updated) {
      successCount++;

      // Evaluate for drafting after successful inventory update
      await evaluateProductForDrafting(
        inventoryItemId,
        locationId,
        storeCode,
        logger
      );
    }

    // Log progress every 10 items
    if ((i + 1) % 10 === 0 || i === inventoryUpdates.length - 1) {
      logger(
        storeCode,
        `📊 Progress: ${
          i + 1
        }/${total} processed (${successCount} successful)`
      );

      // Show rate limiter status
      const status = rateLimiter.getStatus();
      logger(
        storeCode,
        `⚡ Rate limit status: REST: ${status.restCallsInLastSecond}/${status.restLimit}/s, GraphQL: ${status.graphqlPointsUsed}/${status.graphqlLimit}/min, Queue: ${status.queueLength}`
      );
    }
  }

  logger(
    storeCode,
    `🎉 Batch update complete: ${successCount}/${total} items successfully processed`
  );
  return successCount;
}

// Get rate limiter status for monitoring
function getRateLimiterStatus() {
  const status = {};

  for (const [storeCode, rateLimiter] of rateLimiters.entries()) {
    status[storeCode] = rateLimiter.getStatus();
  }

  // Also provide aggregated status
  const aggregated = {
    totalQueueLength: 0,
    activeStores: rateLimiters.size,
    stores: status,
  };

  for (const storeStatus of Object.values(status)) {
    aggregated.totalQueueLength += storeStatus.queueLength;
  }

  return aggregated;
}

module.exports = {
  getInventoryItemIdsBySKUs,
  updateInventoryLevel,
  evaluateProductForDrafting,
  batchUpdateInventory,
  getRateLimiterStatus,
};
