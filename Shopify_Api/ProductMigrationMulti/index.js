const sql = require('mssql');
require('dotenv').config();
const axios = require('axios');
const shop = process.env.Shopify_Shop_Name;
const accessToken = process.env.Shopify_Admin_Api_Access_Token;
const fs = require('fs');
const { log } = require('console');

const createProductMutation = `
mutation CreateProduct($input: ProductInput!) {
  productCreate(input: $input) {
    product {
      id
      handle
      variants(first: 100) {
        edges {
          node {
            id
            sku
          }
        }
      }
    }
    userErrors {
      field
      message
    }
  }
}
`;

const productCreateMediaMutation = `
  mutation createMedia($media: [CreateMediaInput!]!, $productId: ID!) {
    productCreateMedia(media: $media, productId: $productId) {
      media {
        ... on MediaImage {
          id
          image {
            src
          }
        }
      }
      mediaUserErrors {
        field
        message
      }
    }
  }
`;

const metafieldMutation = `
    mutation SetMetafields($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          key
          namespace
          value
          type
        }
        userErrors {
          field
          message
        }
      }
    }
  `;

const logError = (msg) => {
  const timestamp = new Date().toISOString();
  fs.appendFileSync('log.txt', `[${timestamp}] ${msg}\n`);
};

const connection = {
  user: process.env.SQL_USER,
  password: process.env.SQL_PASSWORD,
  database: process.env.SQL_DATABASE,
  server: process.env.SQL_IP,
  options: {
    encrypt: false,
    trustServerCertificate: true,
  },
};

const getDataByMultiStyleCode = async (multiStyleCode) => {
  const db = await sql.connect(connection);
  const query = `
SELECT
    Styles.Code,
    LOWER(
        CONCAT(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                Styles.StyleDesc, 
                ' ', '-'), 
                '&', 'and'), 
                '/', '-'), 
                '''', ''), 
                '.', ''), 
                ',', ''), 
                '"', ''
            ),
            '-', 
            '${multiStyleCode}'
        )
    ) AS Handle,

    Styles.StyleDesc AS Title,
    Styles.StyleLongDesc AS 'Body (HTML)',
    'Malani Jewelers' AS Vendor,
    'Apparel & Accessories > Jewelry >' AS 'Product Category',
    '' AS Type,
    '' AS Tag,
    'TRUE' AS Published,
    '' AS 'Option1 Name',
    '' AS 'Option1 Value',
    '' AS 'Option1 Linked To',
    '' AS 'Option2 Name',
    '' AS 'Option2 Value',
    '' AS 'Option2 Linked To',
    '' AS 'Option3 Name',
    '' AS 'Option3 Value',
    '' AS 'Option3 Linked To',
    Styles.SKUCode AS 'Variant SKU',
    Styles.StyleGrossWt AS 'Variant Grams',
    'Shopify' AS 'Variant Inventory Tracker',
    '' AS 'Variant Inventory Qty',
    'deny' AS 'Variant Inventory Policy',
    'Manual' AS 'Variant Fulfillment Service',
    Styles.CustPrice AS 'Variant Price',
    Styles.TagPrice AS 'Variant Compare At Price',
    'TRUE' AS 'Variant Requires Shipping',
    'TRUE' AS 'Variant Taxable',
    '' AS 'Variant Barcode',
    '' AS 'Image Src',
    '' AS 'Image Position',
    '' AS 'Image Alt Text',
    '' AS 'Gift Card',
    Styles.StyleDesc AS 'SEO Title',
    Styles.StyleLongDesc AS 'SEO Description',

    -- Watch Fields
    Styles.AttribField148 AS 'Watch Papers Included (product.metafields.custom.watch_papers_included)',
    Styles.AttribField144 AS 'Watch Bracelet Type (product.metafields.sku.watch_bracelet_type)',
    Styles.AttribField146 AS 'Watch Case Size (product.metafields.sku.watch_case_size)',
    Styles.AttribField145 AS 'Watch Condition (product.metafields.sku.watch_condition)',
    Styles.AttribField147 AS 'Watch Bezel Type (product.metafields.sku.watch_bezel_type)',
    Styles.AttribField109 AS 'Watch Disclaimer (product.metafields.sku.watch_disclaimer)',

    -- Bangle Fields
    Styles.AttribField100 AS 'Bangle Size (product.metafields.sku.bangle_size)',
    Styles.AttribField111 AS 'Bangle/Bracelet Size Adjustable up-to (product.metafields.sku.bangle_bracelet_size_adjustable_up_to)',
    Styles.AttribField101 AS 'Bangle Inner Diameter (product.metafields.sku.bangle_inner_diameter)',
    Styles.AttribField103 AS 'Bangle Design Height (product.metafields.sku.bangle_design_height)',
    Styles.AttribField102 AS 'Bangle Width (product.metafields.sku.bangle_width)',
    Styles.AttribField106 AS 'Bangle/Bracelet Type (product.metafields.sku.bangle_bracelet_type)',

    -- Category
    CSC3.CatSubCat AS 'Category (product.metafields.sku.category)',
    CONCAT(
        CASE WHEN CSC3.CatSubCat IS NOT NULL THEN CSC3.CatSubCat ELSE '' END,
        CASE WHEN CSC3.CatSubCat IS NOT NULL THEN '->' ELSE '' END,
        CASE WHEN CSC2.CatSubCat IS NOT NULL THEN CSC2.CatSubCat ELSE '' END,
        CASE WHEN CSC2.CatSubCat IS NOT NULL AND CatSubCats.CatSubCat IS NOT NULL THEN '->' ELSE '' END,
        COALESCE(CatSubCats.CatSubCat, '')
    ) AS 'Sub Category (product.metafields.sku.sub_category)',

    -- Remaining Shopify Fields
    Styles.AttribField87 AS 'Center Diamond Weight (product.metafields.sku.center_diamond_weight)',
    Styles.AttribField118 AS 'Certificate # 1 (product.metafields.sku.certificate_1)',
    Styles.AttribField132 AS 'Certificate # 2 (product.metafields.sku.certificate_2)',
    Styles.AttribField133 AS 'Certificate # 3 (product.metafields.sku.certificate_3)',
    Styles.AttribField119 AS 'Certification Type (product.metafields.sku.certification_type)',
    CASE 
  WHEN Styles.AttribField104 = 'Yes' THEN 'TRUE' 
  WHEN Styles.AttribField104 = 'No' THEN 'FALSE' 
  ELSE '' 
END AS 'Chain included in the price (product.metafields.sku.chain_included_in_the_price)',
    Styles.AttribField117 AS 'Chain Length (product.metafields.sku.chain_length)',
    Styles.AttribField110 AS 'Changeable Stones Included (product.metafields.sku.changeable_stones_included)',
    Classcodes.ClassCode AS 'Classcode (product.metafields.sku.classcode)',
    CASE WHEN Styles.IsCloseOut = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Close out (product.metafields.sku.close_out)',
    Col.CollectionName AS 'Collection (product.metafields.sku.collection)',
    Color.LongData AS 'Color (product.metafields.sku.color)',
    Styles.IsCustomDesign AS 'Customizable (product.metafields.sku.customizable)',
    Styles.AttribField120 AS 'DC (product.metafields.sku.dc)',
    Styles.AttribField90 AS 'Diamond Clarity (product.metafields.sku.diamond_clarity)',
    Styles.AttribField91 AS 'Diamond Color (product.metafields.sku.diamond_color)',
    Styles.AttribField89 AS 'Diamond Total Pcs (product.metafields.sku.diamond_total_pcs)',
    Styles.AttribField88 AS 'Diamond Total Weight (product.metafields.sku.diamond_total_weight)',
    Styles.AttribField112 AS 'Diamond Type (product.metafields.sku.diamond_type)',
    Styles.AttribField113 AS 'Gemstones Type (product.metafields.sku.gemstones_type)',
    Styles.AttribField134 AS 'Disclaimer (product.metafields.sku.disclaimer)',
    Styles.AttribField95 AS 'Earrings Length (product.metafields.sku.earrings_length)',
    Styles.AttribField96 AS 'Earrings Width (product.metafields.sku.earrings_width)',
    Styles.AttribField105 AS 'Earring Post Type (product.metafields.sku.earring_post_type)',
    CONVERT(varchar(10), Styles.StyleEntryDate, 120) AS 'Entry Date (product.metafields.sku.entryDate)',
    CONVERT(varchar(10), Styles.StyleUploadDate, 120) AS 'Upload Date (product.metafields.sku.upload_date)',
    Styles.AttribField84 AS 'Finish (product.metafields.sku.finish)',
    GoldKarat.LongData AS 'Gold Karat (product.metafields.sku.gold_karat)',
    Styles.StyleGrossWt AS 'Gross Weight (product.metafields.sku.grossWeight)',
    Styles.IsHotSeller AS 'Hot Seller (product.metafields.sku.hot_seller)',
    Styles.AttribField86 AS 'Jewelry For (product.metafields.sku.jewelry_for)',
    Styles.AttribField115 AS 'Jewelry Type (product.metafields.sku.jewelry_type)',
    Styles.AttribField92 AS 'Length (product.metafields.sku.length)',
    CASE WHEN Styles.IsNewArrived = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'New Arrival (product.metafields.sku.new_arrival)',
    Styles.AttribField107 AS 'Nose Pin Type (product.metafields.sku.nose_pin_type)',
    Styles.AttribField83 AS 'Pendant Length (product.metafields.sku.pendant_length)',
    Styles.AttribField94 AS 'Pendant Width (product.metafields.sku.pendant_width)',
    Styles.PerGramOrDisc AS 'Per Gram Or Disc (product.metafields.sku.per_gram_or_disc)',
    CASE WHEN Styles.ShowPriceFallFlag = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Price Fall (product.metafields.sku.price_fall)',
    Styles.AttribField98 AS 'Ring Design Height (product.metafields.sku.ring_design_height)',
    Styles.AttribField97 AS 'Ring Size (product.metafields.sku.ring_size)',
    Styles.AttribField108 AS 'Ring Type (product.metafields.sku.ring_type)',
    Styles.AttribField99 AS 'Ring Width (product.metafields.sku.ring_width)',
    CASE WHEN Styles.ShowRetailPrice = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Show Tag Price (product.metafields.sku.show_tag_price)',
    Styles.TagPrice AS 'Tag Price (product.metafields.sku.tag_price)',
    Vendors.VendorName AS 'Vendor (product.metafields.sku.vendor)',
    Styles.VendStyleCode AS 'Vendor Style (product.metafields.sku.vendorStyle)',
    Styles.AttribField93 AS 'Width (product.metafields.sku.width)',
    Styles.AttribField85 AS 'Number Of Pieces (product.metafields.custom.number_of_pieces)',
    CASE WHEN Styles.AutoUpdatePrice = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Auto Update Price (product.metafields.sku.autoUpdatePrice)'

    FROM Styles
    LEFT JOIN CatSubCats ON Styles.SubCatCode = CatSubCats.Code
    LEFT JOIN CatSubCats AS CSC2 ON CatSubCats.ParentCode = CSC2.Code
    LEFT JOIN CatSubCats AS CSC3 ON CSC2.ParentCode = CSC3.Code
    LEFT JOIN Vendors ON Styles.VendCode = Vendors.Code
    LEFT JOIN ClassCodes ON Styles.ClassCode = ClassCodes.Code
    LEFT JOIN CommonMastersData AS GoldKarat ON Styles.GoldKt = GoldKarat.Code
    LEFT JOIN CommonMastersData AS Color ON Styles.Color = Color.Code
    OUTER APPLY (
        SELECT TOP 1 c.CollectionName
        FROM CollectionSKUMapping m
        LEFT JOIN Collections c ON m.CollectionCode = c.Code
        WHERE m.StyleCode = Styles.Code
        ORDER BY c.CollectionName
    ) Col

    WHERE
        Styles.StockQty = 1 AND 
        Styles.Hidden = 0 AND 
        Styles.Purchasable = 1 AND 
        Styles.isDeleted IS NULL AND
        (Styles.AttribField141 = '${multiStyleCode}'
        OR
        Styles.MultiSkuCode = '${multiStyleCode}')

  `;
  const result = await db.request().query(query);
  return result.recordset;
};

const getImageData = async (StyleCode) => {
  const db = await sql.connect(connection);
  const query = `
    Select * from dbo.SKUImageMapping
    Where StyleCode = '${StyleCode}'

  `;
  const result = await db.request().query(query);
  return result.recordset;
};

const graphQL = async (query, variables) => {
  try {
    const res = await axios.post(
      `https://${shop}.myshopify.com/admin/api/2023-10/graphql.json`,
      { query, variables },
      {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json',
        },
      }
    );
    return res.data;
  } catch (err) {
    logError(`GraphQL Error: ${err.message}`);
    return null;
  }
};

const uploadGroupedProduct = async (variants) => {
  const baseProduct = variants[0];

  const input = {
    title: baseProduct.Title,
    bodyHtml: baseProduct['Body (HTML)'],
    vendor: baseProduct.Vendor,
    handle: baseProduct.Handle,
    published: true,
    productType: baseProduct.Type || 'Jewelry',
    productCategory: {
      productTaxonomyNodeId: 'gid://shopify/ProductTaxonomyNode/972',
    },
    options: ['Available Options'],
    variants: variants.map((v) => {
      const price =
        parseFloat(v['Variant Price'])?.toFixed(2) || '0.00';
      const size = v['Ring Size (product.metafields.sku.ring_size)']
        ? `${v['Ring Size (product.metafields.sku.ring_size)']}`
        : v['Bangle Size (product.metafields.sku.bangle_size)']
        ? `${v['Bangle Size (product.metafields.sku.bangle_size)']}`
        : v[
            'Diamond Total Weight (product.metafields.sku.diamond_total_weight)'
          ]
        ? `${parseFloat(
            v[
              'Diamond Total Weight (product.metafields.sku.diamond_total_weight)'
            ]
          ).toFixed(2)} ct`
        : v['Length (product.metafields.sku.length)']
        ? `${v['Length (product.metafields.sku.length)']} inches`
        : v['Gross Weight (product.metafields.sku.grossWeight)']
        ? `${parseFloat(
            v['Gross Weight (product.metafields.sku.grossWeight)']
          ).toFixed(2)} g`
        : 'N/A';

      return {
        sku: v['Variant SKU'],
        price,
        compareAtPrice:
          v['Variant Compare At Price']?.toString() || null,
        inventoryManagement: 'SHOPIFY',
        inventoryPolicy: 'DENY',
        taxable: true,
        requiresShipping: true,
        weight: Number(v['Variant Grams']) || 0,
        weightUnit: 'GRAMS',
        options: [`${v['Variant SKU']} | $${price} | ${size}`],
      };
    }),
  };

  const productCreateRes = await graphQL(createProductMutation, {
    input,
  });
  console.dir(productCreateRes, { depth: null });

  const productId =
    productCreateRes?.data?.productCreate?.product?.id;
  const variantEdges =
    productCreateRes?.data?.productCreate?.product?.variants?.edges ||
    [];

  const variantIdMap = variantEdges.map(({ node }) => ({
    id: node.id,
    sku: node.sku,
  }));

  if (!productId) {
    logError(
      `Failed to create product for handle ${baseProduct.Handle}`
    );
    return;
  }

  // Upload product-level metafields (only once, from base)
  await uploadProductMetafields(productId, baseProduct);

  // Pull image data once
  const imageData = await getImageData(baseProduct.Code);
  const imageArr = imageData.map(
    (img) =>
      `https://www.malanijewelers.com/TransactionImages/Styles/large/${img.LargeImg}`
  );

  // Upload single image once
  await attachImagesToProduct(
    productId,
    imageArr,
    baseProduct['Variant SKU']
  );

  for (let i = 0; i < variants.length; i++) {
    const originalVariant = variants[i];
    const matchingShopifyVariant = variantIdMap.find(
      (v) => v.sku === originalVariant['Variant SKU']
    );

    if (!matchingShopifyVariant) {
      logError(
        `❌ No Shopify variant found for SKU ${originalVariant['Variant SKU']}`
      );
      continue;
    }

    originalVariant.id = matchingShopifyVariant.id;
    await uploadVariantMetafields(productId, i, originalVariant);
  }
};

const uploadProductMetafields = async (productId, product) => {
  // Prepare metafields
  const metafields = [
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'classcode',
      type: 'number_integer',
      value:
        product[
          'Classcode (product.metafields.sku.classcode)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'vendor',
      type: 'single_line_text_field',
      value: product['Vendor (product.metafields.sku.vendor)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'vendorStyle',
      type: 'single_line_text_field',
      value:
        product[
          'Vendor Style (product.metafields.sku.vendorStyle)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'vjsDescription1',
      type: 'single_line_text_field',
      value:
        product[
          'VJS Description 1 (product.metafields.sku.vjsDescription1)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'vjsDescription2',
      type: 'single_line_text_field',
      value:
        product[
          'VJS Description 2 (product.metafields.sku.vjsDescription2)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'grossWeight',
      type: 'number_decimal',
      value:
        product[
          'Gross Weight (product.metafields.sku.grossWeight)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'entryDate',
      type: 'date',
      value:
        product[
          'Entry Date (product.metafields.sku.entryDate)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'dc',
      type: 'single_line_text_field',
      value: product['DC (product.metafields.sku.dc)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'tag_price',
      type: 'money',
      value: JSON.stringify({
        amount:
          product[
            'Tag Price (product.metafields.sku.tag_price)'
          ]?.toFixed(2) || '0.00',
        currency_code: 'USD',
      }),
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'category',
      type: 'single_line_text_field',
      value:
        toProperCase(
          product['Category (product.metafields.sku.category)']
        ) || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'sub_category',
      type: 'single_line_text_field',
      value:
        product[
          'Sub Category (product.metafields.sku.sub_category)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'length',
      type: 'number_decimal',
      value:
        product[
          'Length (product.metafields.sku.length)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'width',
      type: 'number_decimal',
      value:
        product['Width (product.metafields.sku.width)']?.toString() ||
        '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'chain_length',
      type: 'single_line_text_field',
      value:
        product[
          'Chain Length (product.metafields.sku.chain_length)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'chain_included_in_the_price',
      type: 'boolean',
      value:
        product[
          'Chain included in the price (product.metafields.sku.chain_included_in_the_price)'
        ] === 'TRUE'
          ? true
          : product[
              'Chain included in the price (product.metafields.sku.chain_included_in_the_price)'
            ] === 'FALSE'
          ? false
          : null,
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'pendant_length',
      type: 'number_decimal',
      value:
        product[
          'Pendant Length (product.metafields.sku.pendant_length)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'pendant_width',
      type: 'number_decimal',
      value:
        product[
          'Pendant Width (product.metafields.sku.pendant_width)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'earrings_length',
      type: 'number_decimal',
      value:
        product[
          'Earrings Length (product.metafields.sku.earrings_length)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'earrings_width',
      type: 'number_decimal',
      value:
        product[
          'Earrings Width (product.metafields.sku.earrings_width)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'earring_post_type',
      type: 'single_line_text_field',
      value:
        toProperCase(
          product[
            'Earring Post Type (product.metafields.sku.earring_post_type)'
          ]
        ) || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'ring_size',
      type: 'single_line_text_field',
      value:
        product['Ring Size (product.metafields.sku.ring_size)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'ring_design_height',
      type: 'number_decimal',
      value:
        product[
          'Ring Design Height (product.metafields.sku.ring_design_height)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'ring_width',
      type: 'number_decimal',
      value:
        product[
          'Ring Width (product.metafields.sku.ring_width)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'ring_type',
      type: 'single_line_text_field',
      value:
        product['Ring Type (product.metafields.sku.ring_type)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'bangle_size',
      type: 'single_line_text_field',
      value:
        product['Bangle Size (product.metafields.sku.bangle_size)'] ||
        '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'bangle_inner_diameter',
      type: 'number_decimal',
      value:
        product[
          'Bangle Inner Diameter (product.metafields.sku.bangle_inner_diameter)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'bangle_width',
      type: 'number_decimal',
      value:
        product[
          'Bangle Width (product.metafields.sku.bangle_width)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'bangle_design_height',
      type: 'number_decimal',
      value:
        product[
          'Bangle Design Height (product.metafields.sku.bangle_design_height)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'bangle_bracelet_type',
      type: 'single_line_text_field',
      value:
        product[
          'Bangle/Bracelet Type (product.metafields.sku.bangle_bracelet_type)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'bangle_bracelet_size_adjustable_up_to',
      type: 'single_line_text_field',
      value:
        product[
          'Bangle/Bracelet Size Adjustable up-to (product.metafields.sku.bangle_bracelet_size_adjustable_up_to)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'nose_pin_type',
      type: 'single_line_text_field',
      value:
        toProperCase(
          product[
            'Nose Pin Type (product.metafields.sku.nose_pin_type)'
          ]
        ) || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'collection',
      type: 'single_line_text_field',
      value:
        product['Collection (product.metafields.sku.collection)'] ||
        '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'jewelry_for',
      type: 'single_line_text_field',
      value: toProperCase(
        product['Jewelry For (product.metafields.sku.jewelry_for)'] ||
          ''
      ).trim(),
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'jewelry_type',
      type: 'single_line_text_field',
      value:
        product[
          'Jewelry Type (product.metafields.sku.jewelry_type)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'gold_karat',
      type: 'single_line_text_field',
      value:
        product['Gold Karat (product.metafields.sku.gold_karat)'] ||
        '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'color',
      type: 'single_line_text_field',
      value: product['Color (product.metafields.sku.color)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'finish',
      type: 'single_line_text_field',
      value: product['Finish (product.metafields.sku.finish)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'disclaimer',
      type: 'single_line_text_field',
      value:
        product['Disclaimer (product.metafields.sku.disclaimer)'] ||
        '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'upload_date',
      type: 'date',
      value:
        product[
          'Upload Date (product.metafields.sku.upload_date)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'per_gram_or_disc',
      type: 'single_line_text_field',
      value:
        product[
          'Per Gram Or Disc (product.metafields.sku.per_gram_or_disc)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'center_diamond_weight',
      type: 'number_decimal',
      value:
        product[
          'Center Diamond Weight (product.metafields.sku.center_diamond_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'diamond_total_weight',
      type: 'number_decimal',
      value:
        product[
          'Diamond Total Weight (product.metafields.sku.diamond_total_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'diamond_total_pcs',
      type: 'number_integer',
      value:
        product[
          'Diamond Total Pcs (product.metafields.sku.diamond_total_pcs)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'diamond_clarity',
      type: 'single_line_text_field',
      value:
        product[
          'Diamond Clarity (product.metafields.sku.diamond_clarity)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'diamond_color',
      type: 'single_line_text_field',
      value:
        product[
          'Diamond Color (product.metafields.sku.diamond_color)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'diamond_type',
      type: 'single_line_text_field',
      value:
        product[
          'Diamond Type (product.metafields.sku.diamond_type)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'certification_type',
      type: 'single_line_text_field',
      value:
        product[
          'Certification Type (product.metafields.sku.certification_type)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'certificate_1',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 1 (product.metafields.sku.certificate_1)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'certificate_2',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 2 (product.metafields.sku.certificate_2)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'certificate_3',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 3 (product.metafields.sku.certificate_3)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'certificate_4',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 4 (product.metafields.sku.certificate_4)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'certificate_5',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 5 (product.metafields.sku.certificate_5)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'gemstones_type',
      type: 'list.single_line_text_field',
      value: product[
        'Gemstones Type (product.metafields.sku.gemstones_type)'
      ]
        ? JSON.stringify(
            product[
              'Gemstones Type (product.metafields.sku.gemstones_type)'
            ]
              .split(/\s{2,}/) // split by 2+ spaces
              .map((s) => s.trim())
              .filter(Boolean)
          )
        : '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'changeable_stones_included',
      type: 'single_line_text_field',
      value:
        product[
          'Changeable Stones Included (product.metafields.sku.changeable_stones_included)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'customizable',
      type: 'boolean',
      value:
        product[
          'Customizable (product.metafields.sku.customizable)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'price_fall',
      type: 'boolean',
      value:
        product[
          'Price Fall (product.metafields.sku.price_fall)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'show_tag_price',
      type: 'boolean',
      value:
        product[
          'Show Tag Price (product.metafields.sku.show_tag_price)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'hot_seller',
      type: 'boolean',
      value:
        product[
          'Hot Seller (product.metafields.sku.hot_seller)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'new_arrival',
      type: 'boolean',
      value:
        product[
          'New Arrival (product.metafields.sku.new_arrival)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'close_out',
      type: 'boolean',
      value:
        product[
          'Close out (product.metafields.sku.close_out)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'watch_condition',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Condition (product.metafields.sku.watch_condition)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'watch_bracelet_type',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Bracelet Type (product.metafields.sku.watch_bracelet_type)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'watch_case_size',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Case Size (product.metafields.sku.watch_case_size)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'watch_bezel_type',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Bezel Type (product.metafields.sku.watch_bezel_type)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'custom',
      key: 'watch_papers_included',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Papers Included (product.metafields.custom.watch_papers_included)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'watch_disclaimer',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Disclaimer (product.metafields.sku.watch_disclaimer)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'mm-google-shopping',
      key: 'custom_product',
      type: 'boolean',
      value:
        product[
          'Google: Custom Product (product.metafields.mm-google-shopping.custom_product)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'shopify--discovery--product_search_boost',
      key: 'queries',
      type: 'list.single_line_text_field',
      value:
        product[
          'Search product boosts (product.metafields.shopify--discovery--product_search_boost.queries)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'shopify--discovery--product_recommendation',
      key: 'related_products',
      type: 'list.product_reference',
      value:
        product[
          'Related products (product.metafields.shopify--discovery--product_recommendation.related_products)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'shopify--discovery--product_recommendation',
      key: 'related_products_display',
      type: 'single_line_text_field',
      value:
        product[
          'Related products settings (product.metafields.shopify--discovery--product_recommendation.related_products_display)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'shopify--discovery--product_recommendation',
      key: 'complementary_products',
      type: 'list.product_reference',
      value:
        product[
          'Complementary products (product.metafields.shopify--discovery--product_recommendation.complementary_products)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'gemstones_weight',
      type: 'number_decimal',
      value:
        product[
          'Gemstones Weight (product.metafields.sku.gemstones_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: productId,
      namespace: 'custom',
      key: 'number_of_pieces',
      type: 'single_line_text_field',
      value:
        product[
          'Number Of Pieces (product.metafields.custom.number_of_pieces)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'metal_type',
      type: 'single_line_text_field',
      value:
        product['metal type (product.metafields.sku.metal_type)'] ||
        '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'autoUpdatePrice',
      type: 'boolean',
      value:
        product[
          'Auto Update Price (product.metafields.sku.autoUpdatePrice)'
        ]?.toString() || '',
    },
  ];

  // Remove empty/null/undefined metafield values
  const validMetafields = metafields.filter(
    (m) => m.value !== null && m.value !== undefined && m.value !== ''
  );

  // Ensure all non-JSON values are stringified
  validMetafields.forEach((m) => {
    if (m.type === 'json') return; // skip JSON, leave it alone
    if (m.type === 'boolean') return; // skip boolean, keep as true/false

    if (typeof m.value !== 'string') {
      m.value = m.value.toString();
    }
  });

  // Helper to chunk metafields array into groups of 25
  function chunkArray(array, size) {
    const result = [];
    for (let i = 0; i < array.length; i += size) {
      result.push(array.slice(i, i + size));
    }
    return result;
  }

  const metafieldChunks = chunkArray(validMetafields, 25);
  let allErrors = [];

  for (let chunk of metafieldChunks) {
    let attempt = 0;
    let success = false;

    while (!success && chunk.length > 0 && attempt < 5) {
      const metafieldRes = await graphQL(metafieldMutation, {
        metafields: chunk,
      });

      const userErrors =
        metafieldRes?.data?.metafieldsSet?.userErrors || [];

      if (userErrors.length === 0) {
        success = true;
        logError(
          `✅ Uploaded ${chunk.length} metafields for SKU ${product['Variant SKU']}`
        );
        break;
      }

      attempt++;
      logError(
        `⚠️ Attempt ${attempt}: ${userErrors.length} metafield errors`
      );

      const badIndexes = new Set(
        userErrors
          .map((e) => {
            const match = e.field?.[0]?.match(/metafields\.(\d+)/);
            return match ? parseInt(match[1], 10) : null;
          })
          .filter((i) => i !== null)
      );

      // Remove in reverse to avoid shifting indexes
      const sortedIndexes = [...badIndexes].sort((a, b) => b - a);
      for (const i of sortedIndexes) {
        const removed = chunk.splice(i, 1)[0];
        logError(
          `🛑 Removed invalid metafield from chunk:\n` +
            `  → Key: ${removed.key}\n` +
            `  → Value: ${JSON.stringify(removed.value)}`
        );
      }
    }

    if (!success) {
      logError(
        `❌ Could not upload any metafields in chunk for SKU ${product['Variant SKU']}`
      );
      allErrors.push(...chunk); // remaining chunk considered failed
    }
  }

  // Final log
  if (allErrors.length > 0) {
    logError(
      `Metafield errors for SKU ${
        product['Variant SKU']
      }: ${JSON.stringify(allErrors)}`
    );
  } else {
    logError(
      `✅ Uploaded SKU ${product['Variant SKU']} with metafields`
    );
  }
};

const uploadVariantMetafields = async (productId, index, variant) => {
  const fields = [
    'bangle_size',
    'ring_size',
    'length',
    'gross_weight',
    'entry_date',
    'upload_date',
    'tag_price',
  ];

  const metafields = [
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'bangle_size',
      type: 'single_line_text_field',
      value:
        variant[
          'Bangle Size (product.metafields.sku.bangle_size)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'ring_size',
      type: 'single_line_text_field',
      value:
        variant[
          'Ring Size (product.metafields.sku.ring_size)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'length',
      type: 'number_decimal',
      value:
        variant[
          'Length (product.metafields.sku.length)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'diamond_total_weight',
      type: 'number_decimal',
      value:
        variant[
          'Diamond Total Weight (product.metafields.sku.diamond_total_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'gross_weight',
      type: 'number_decimal',
      value: variant['Variant Grams']?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'entry_date',
      type: 'date',
      value:
        variant['Entry Date (product.metafields.sku.entryDate)'] ||
        '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'upload_date',
      type: 'date',
      value:
        variant['Upload Date (product.metafields.sku.upload_date)'] ||
        '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'tag_price',
      type: 'money',
      value: JSON.stringify({
        amount:
          variant[
            'Tag Price (product.metafields.sku.tag_price)'
          ]?.toFixed(2) || '0.00',
        currency_code: 'USD',
      }),
    },
  ].filter((m) => m.value);

  const chunks = [metafields]; // can add chunking if >25 later
  for (const chunk of chunks) {
    const res = await graphQL(metafieldMutation, {
      metafields: chunk,
    });
    const errors = res?.data?.metafieldsSet?.userErrors;
    console.dir(res, { depth: null });

    if (errors?.length)
      logError(
        `Variant metafield errors [${
          variant['Variant SKU']
        }]: ${JSON.stringify(errors)}`
      );
  }
};
const attachImagesToProduct = async (productId, imageUrls, sku) => {
  if (!imageUrls?.length) return;

  const media = imageUrls.map((src) => ({
    originalSource: src,
    mediaContentType: 'IMAGE',
    alt: sku, // still useful
  }));

  const result = await graphQL(productCreateMediaMutation, {
    productId,
    media,
  });

  const errors = result?.data?.productCreateMedia?.mediaUserErrors;
  if (errors?.length > 0) {
    console.dir(errors, { depth: null });
    logError(
      `Image upload errors for SKU ${sku}: ${JSON.stringify(errors)}`
    );
  } else {
    logError(`🖼️ Images uploaded for SKU ${sku}`);
  }
};

(async () => {
  const multiStyleCodes = [
    '500-00627-MIS-W-W',
    '500-00628-MIS-W-W',
    '500-00626-MIS-W-W',
  ]; // sequential input, one at a time

  for (const code of multiStyleCodes) {
    logError(`Processing MultiStyleCode: ${code}`);
    const variants = await getDataByMultiStyleCode(code);

    if (!variants.length) {
      logError(`No records found for MultiStyleCode: ${code}`);
      continue;
    }

    await uploadGroupedProduct(variants);
    logError(`Finished MultiStyleCode: ${code}`);
  }

  console.log('🎉 Multi-variant upload complete');
})();

function toProperCase(str) {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/\w\S*/g, (txt) => {
    return txt.charAt(0).toUpperCase() + txt.slice(1).toLowerCase();
  });
}
