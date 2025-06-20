const axios = require('axios');
require('dotenv').config();

const SHOP = process.env.Shopify_Shop_Name.replace(/['"]+/g, '');
const ACCESS_TOKEN = process.env.Shopify_Admin_Api_Access_Token;

// Batched SKU lookup via GraphQL
async function getInventoryItemIdsBySKUs(skuList) {
  const queryString = skuList.map((sku) => `sku:${sku}`).join(' OR ');
  const query = `
    query {
      productVariants(first: 100, query: "${queryString}") {
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
    const res = await axios.post(
      `https://${SHOP}.myshopify.com/admin/api/2024-04/graphql.json`,
      { query },
      {
        headers: {
          'X-Shopify-Access-Token': ACCESS_TOKEN,
          'Content-Type': 'application/json',
        },
      }
    );

    const variants = res.data.data.productVariants.edges;
    const map = {};

    for (const v of variants) {
      const inventoryItemIdGlobal = v.node.inventoryItem.id;
      const inventoryItemId = inventoryItemIdGlobal.split('/').pop(); // ✅ parse numeric ID here
      map[v.node.sku] = inventoryItemId;
    }

    return map;
  } catch (err) {
    console.error(
      'GraphQL error:',
      err?.response?.data || err.message
    );
    return {};
  }
}

// Inventory update (still REST API)
async function updateInventoryLevel(
  inventoryItemId,
  quantity,
  locationId
) {
  try {
    await axios.post(
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
      }
    );
    return true;
  } catch (err) {
    console.error(
      'Error updating inventory level:',
      err?.response?.data || err.message
    );
    return false;
  }
}

// This function triggers after inventory update
async function evaluateProductForDrafting(
  inventoryItemId,
  locationId,
  storeCode,
  logger
) {
  try {
    // Step 1 — Get variant by inventoryItemId (GraphQL)
    const queryVariant = `
      query {
        inventoryItem(id: "gid://shopify/InventoryItem/${inventoryItemId}") {
          variant {
            id
            product {
              id
              handle
              title
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

    const res = await axios.post(
      `https://${SHOP}.myshopify.com/admin/api/2024-04/graphql.json`,
      { query: queryVariant },
      {
        headers: {
          'X-Shopify-Access-Token': ACCESS_TOKEN,
          'Content-Type': 'application/json',
        },
      }
    );

    const productNode = res.data.data.inventoryItem.variant.product;

    const variants = productNode.variants.edges;
    const productId = productNode.id;
    const productHandle = productNode.handle;

    // Step 2 — Check if any sibling has inventory > 0
    const anyInStock = variants.some(
      (v) => v.node.inventoryQuantity > 0
    );

    if (anyInStock) {
      logger(
        storeCode,
        `Product ${productHandle} still has variants in stock — no action.`
      );
      return false;
    }

    // Step 3 — Set product to draft (REST)
    const productNumericId = productId.split('/').pop();

    await axios.put(
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
      }
    );

    logger(storeCode, `Product ${productHandle} set to draft.`);

    // Step 4 — Create redirect
    const collections = productNode.collections.edges;
    if (collections.length === 0) {
      logger(
        storeCode,
        `Product ${productHandle} has no collections, skipping redirect.`
      );
      return true;
    }

    // Pick longest collection title
    collections.sort(
      (a, b) => b.node.title.length - a.node.title.length
    );
    const targetCollection = collections[0].node.handle;

    await axios.post(
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
      }
    );

    logger(
      storeCode,
      `Redirect created for ${productHandle} to /collections/${targetCollection}`
    );

    return true;
  } catch (err) {
    console.error(
      'Error in evaluateProductForDrafting:',
      err?.response?.data || err.message
    );
    logger(
      storeCode,
      `❌ Failed to evaluate product: ${err.message}`
    );
    return false;
  }
}

module.exports = {
  getInventoryItemIdsBySKUs,
  updateInventoryLevel,
  evaluateProductForDrafting,
};
