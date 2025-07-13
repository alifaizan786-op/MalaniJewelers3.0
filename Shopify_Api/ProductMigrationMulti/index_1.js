const sql = require('mssql');
require('dotenv').config();
const axios = require('axios');
const shop = process.env.Shopify_Shop_Name;
const accessToken = process.env.Shopify_Admin_Api_Access_Token;
const fs = require('fs');

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
    Styles.AttribField148 AS 'Watch Papers Included (product.metafields.sku.watch_papers_included)',
    Styles.AttribField144 AS 'Watch Bracelet Type (product.metafields.sku.watch_bracelet_type)',
    Styles.AttribField146 AS 'Watch Case Size (product.metafields.sku.watch_case_size)',
    Styles.AttribField145 AS 'Watch Condition (product.metafields.sku.watch_condition)',
    Styles.AttribField147 AS 'Watch Bezel Type (product.metafields.sku.watch_bezel_type)',
    Styles.AttribField109 AS 'Watch Disclaimer (product.metafields.sku.watch_disclaimer)',

    -- Bangle Fields
    Styles.AttribField100 AS 'Bangle Size (variant.metafields.variant.bangle_size)',
    Styles.AttribField111 AS 'Bangle/Bracelet Size Adjustable up-to (variant.metafields.variant.bangle_bracelet_size_adjustable_up_to)',
	ROUND(Styles.AttribField101 * 25.4, 1) AS 'Bangle Inner Diameter (variant.metafields.variant.bangle_inner_diameter)',
	ROUND(Styles.AttribField103 * 25.4, 1) AS 'Bangle Design Height (product.metafields.sku.bangle_design_height)',
	ROUND(Styles.AttribField102 * 25.4, 1) AS 'Bangle Width (product.metafields.sku.bangle_width)',
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
    Styles.AttribField87 AS 'Center Diamond Weight (variant.metafields.variant.center_diamond_weight)',
    Styles.AttribField118 AS 'Certificate # 1 (variant.metafields.variant.certificate_1)',
    Styles.AttribField132 AS 'Certificate # 2 (variant.metafields.variant.certificate_2)',
    Styles.AttribField133 AS 'Certificate # 3 (variant.metafields.variant.certificate_3)',
    Styles.AttribField119 AS 'Certification Type (variant.metafields.variant.certification_type)',
    CASE 
  WHEN Styles.AttribField104 = 'Yes' THEN 'TRUE' 
  WHEN Styles.AttribField104 = 'No' THEN 'FALSE' 
  ELSE '' 
END AS 'Chain included in the price (product.metafields.sku.chain_included_in_the_price)',
    Styles.AttribField117 AS 'Chain Length (product.metafields.sku.chain_length)',
    Styles.AttribField110 AS 'Changeable Stones Included (product.metafields.sku.changeable_stones_included)',
    Classcodes.ClassCode AS 'Classcode (product.metafields.sku.classcode)',
    CASE WHEN Styles.IsCloseOut = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Close out (variant.metafields.variant.close_out)',
    Col.CollectionName AS 'Collection (product.metafields.sku.collection)',
    Color.LongData AS 'Color (product.metafields.sku.color)',
    Styles.IsCustomDesign AS 'Customizable (product.metafields.sku.customizable)',
    Styles.AttribField120 AS 'DC (variant.metafields.variant.dc)',
    Styles.AttribField90 AS 'Diamond Clarity (product.metafields.sku.diamond_clarity)',
    Styles.AttribField91 AS 'Diamond Color (product.metafields.sku.diamond_color)',
    Styles.AttribField89 AS 'Diamond Total Pcs (variant.metafields.variant.diamond_total_pcs)',
    Styles.AttribField88 AS 'Diamond Total Weight (variant.metafields.variant.diamond_total_weight)',
    Styles.AttribField112 AS 'Diamond Type (product.metafields.sku.diamond_type)',
    Styles.AttribField113 AS 'Gemstones Type (product.metafields.sku.gemstones_type)',
    Styles.AttribField134 AS 'Disclaimer (product.metafields.sku.disclaimer)',
	FORMAT(CAST(Styles.AttribField95 AS DECIMAL(10, 4)) * 25.4, 'N1') AS 'Earrings Length (product.metafields.sku.earrings_length)',
	FORMAT(CAST(Styles.AttribField96 AS DECIMAL(10, 4)) * 25.4, 'N1') AS 'Earrings Width (product.metafields.sku.earrings_width)',
	Styles.AttribField95  AS 'Earrings Length In (product.metafields.sku.earrings_length)',
	Styles.AttribField96 AS 'Earrings Width In (product.metafields.sku.earrings_width)',
    Styles.AttribField105 AS 'Earring Post Type (product.metafields.sku.earring_post_type)',
    CONVERT(varchar(10), Styles.StyleEntryDate, 120) AS 'Entry Date (variant.metafields.variant.entry_date)',
    CONVERT(varchar(10), Styles.StyleUploadDate, 120) AS 'Upload Date (variant.metafields.variant.upload_date)',
    Styles.AttribField84 AS 'Finish (product.metafields.sku.finish)',
    GoldKarat.LongData AS 'Gold Karat (product.metafields.sku.gold_karat)',
    Styles.StyleGrossWt AS 'Gross Weight (variant.metafields.variant.gross_weight)',
    Styles.IsHotSeller AS 'Hot Seller (product.metafields.sku.hot_seller)',
    Styles.AttribField86 AS 'Jewelry For (product.metafields.sku.jewelry_for)',
    Styles.AttribField115 AS 'Jewelry Type (product.metafields.sku.jewelry_type)',
    Styles.AttribField92 AS 'Length (variant.metafields.variant.length)',
    CASE WHEN Styles.IsNewArrived = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'New Arrival (variant.metafields.variant.new_arrival)',
    Styles.AttribField107 AS 'Nose Pin Type (product.metafields.sku.nose_pin_type)',
	FORMAT(CAST(Styles.AttribField83 AS DECIMAL(10, 4)) * 25.4, 'N1') AS 'Pendant Length (product.metafields.sku.pendant_length)',
	FORMAT(CAST(Styles.AttribField94 AS DECIMAL(10, 4)) * 25.4, 'N1') AS 'Pendant Width (product.metafields.sku.pendant_width)',
    Styles.PerGramOrDisc AS 'Per Gram Or Disc (variant.metafields.variant.per_gram_or_disc)',
    CASE WHEN Styles.ShowPriceFallFlag = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Price Fall (variant.metafields.variant.price_fall)',
FORMAT(CAST(Styles.AttribField98 AS DECIMAL(10, 4)) * 25.4, 'N1') AS 'Ring Design Height (product.metafields.sku.ring_design_height)',
    Styles.AttribField97 AS 'Ring Size (variant.metafields.variant.ring_size)',
    Styles.AttribField108 AS 'Ring Type (product.metafields.sku.ring_type)',
FORMAT(CAST(Styles.AttribField99 AS DECIMAL(10, 4)) * 25.4, 'N1') AS 'Ring Width (product.metafields.sku.ring_width)',
    CASE WHEN Styles.ShowRetailPrice = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Show Tag Price (variant.metafields.variant.show_tag_price)',
    Styles.TagPrice AS 'Tag Price (variant.metafields.variant.tag_price)',
    Vendors.VendorName AS 'Vendor (variant.metafields.variant.vendor)',
    Styles.VendStyleCode AS 'Vendor Style (variant.metafields.variant.vendorStyle)',
    Styles.AttribField93 AS 'Width (product.metafields.sku.width)',
    Styles.AttribField85 AS 'Number Of Pieces (product.metafields.sku.number_of_pieces)',
    CASE WHEN Styles.AutoUpdatePrice = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Auto Update Price (variant.metafields.variant.autoUpdatePrice)',
    
    -- Additional fields that might be needed (add these columns if they exist in your DB)
    '' AS 'Certificate # 4 (variant.metafields.variant.certificate_4)',
    '' AS 'Certificate # 5 (variant.metafields.variant.certificate_5)',
    '' AS 'Gemstones Weight (variant.metafields.variant.gemstones_weight)',
    '' AS 'Metal Type (product.metafields.sku.metal_type)',
    '' AS 'Video URL (product.metafields.sku.video_url)',
    '' AS 'View Count (product.metafields.sku.view_count)'

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

const uploadGroupedProduct = async (variants, multiStyleCode) => {
  const baseProduct = variants[0];

  //331 - Apparel & Accessories > Jewelry
  //332 - Apparel & Accessories > Jewelry > Anklets
  //333 - Apparel & Accessories > Jewelry > Body Jewelry
  //334 - Apparel & Accessories > Jewelry > Bracelets
  //335 - Apparel & Accessories > Jewelry > Brooches & Lapel Pins
  //336 - Apparel & Accessories > Jewelry > Charms & Pendants
  //337 - Apparel & Accessories > Jewelry > Earrings
  //338 - Apparel & Accessories > Jewelry > Jewelry Sets
  //339 - Apparel & Accessories > Jewelry > Necklaces
  //340 - Apparel & Accessories > Jewelry > Rings
  //341 - Apparel & Accessories > Jewelry > Watch Accessories
  //345 - Apparel & Accessories > Jewelry > Watches
  //972 - Business & Industrial > Finance & Insurance > Bullion
  //4332 - Religious & Ceremonial > Religious Items
  //539 - Arts & Entertainment > Hobbies & Creative Arts > Collectibles > Rocks & Fossils

  const input = {
    title: baseProduct.Title,
    bodyHtml: baseProduct['Body (HTML)'],
    vendor: baseProduct.Vendor,
    handle: baseProduct.Handle,
    published: true,
    productType: baseProduct.Type || 'Jewelry',
    productCategory: {
      productTaxonomyNodeId: 'gid://shopify/ProductTaxonomyNode/334',
    },
    options: ['Available Options'],
    variants: variants.map((v) => {
      const price =
        parseFloat(v['Variant Price'])?.toFixed(2) || '0.00';
      const weight = `${parseFloat(
        v['Gross Weight (variant.metafields.variant.gross_weight)']
      ).toFixed(2)} g`;

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
        options: [`${v['Variant SKU']} | $${price} | ${weight}`],
      };
    }),
  };

  const productCreateRes = await graphQL(createProductMutation, {
    input,
  });

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
  await uploadProductMetafields(
    productId,
    baseProduct,
    multiStyleCode
  );

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

  // Upload variant metafields for each variant
  for (let i = 0; i < variants.length; i++) {
    const originalVariant = variants[i];

    const matchingShopifyVariant = variantIdMap.find(
      (v) => v.sku === originalVariant['Variant SKU']
    );

    originalVariant.id = matchingShopifyVariant.id;

    if (!matchingShopifyVariant) {
      logError(
        `❌ No Shopify variant found for SKU ${originalVariant['Variant SKU']}`
      );
      continue;
    }

    await uploadVariantMetafields(null, null, originalVariant);
  }
};

const uploadProductMetafields = async (
  productId,
  product,
  multiStyleCode
) => {
  // Only product-level metafields according to new mapping
  const metafields = [
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'multi_style_code',
      type: 'single_line_text_field',
      value: multiStyleCode,
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
      key: 'chain_included_in_the_price',
      type: 'boolean',
      value:
        product[
          'Chain included in the price (product.metafields.sku.chain_included_in_the_price)'
        ]?.toString() || '',
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
      key: 'collection',
      type: 'single_line_text_field',
      value:
        product['Collection (product.metafields.sku.collection)'] ||
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
      key: 'disclaimer',
      type: 'single_line_text_field',
      value:
        product['Disclaimer (product.metafields.sku.disclaimer)'] ||
        '',
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
      key: 'finish',
      type: 'single_line_text_field',
      value: product['Finish (product.metafields.sku.finish)'] || '',
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
      key: 'gold_karat',
      type: 'single_line_text_field',
      value:
        product['Gold Karat (product.metafields.sku.gold_karat)'] ||
        '',
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
      key: 'metal_type',
      type: 'single_line_text_field',
      value:
        product['Metal Type (product.metafields.sku.metal_type)'] ||
        '',
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
      key: 'number_of_pieces',
      type: 'single_line_text_field',
      value:
        product[
          'Number Of Pieces (product.metafields.sku.number_of_pieces)'
        ] || '',
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
      key: 'ring_type',
      type: 'single_line_text_field',
      value:
        product['Ring Type (product.metafields.sku.ring_type)'] || '',
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
      key: 'video_url',
      type: 'url',
      value:
        product['Video URL (product.metafields.sku.video_url)'] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'view_count',
      type: 'number_integer',
      value:
        product[
          'View Count (product.metafields.sku.view_count)'
        ]?.toString() || '',
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
      key: 'watch_disclaimer',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Disclaimer (product.metafields.sku.watch_disclaimer)'
        ] || '',
    },
    {
      ownerId: productId,
      namespace: 'sku',
      key: 'watch_papers_included',
      type: 'single_line_text_field',
      value:
        product[
          'Watch Papers Included (product.metafields.sku.watch_papers_included)'
        ] || '',
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
  ];

  // Remove empty/null/undefined metafield values
  const validMetafields = metafields.filter(
    (m) => m.value !== null && m.value !== undefined && m.value !== ''
  );

  if (validMetafields.length === 0) {
    logError(
      `❌ No valid variant metafields found for SKU ${variant['Variant SKU']}`
    );
    return;
  }

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
          `✅ Uploaded ${chunk.length} product metafields for SKU ${product['Variant SKU']}`
        );
        break;
      }

      attempt++;

      const badIndex = userErrors[0].field[1];

      logError(
        `⚠️ Attempt ${attempt}: ${
          userErrors.length
        } product metafield errors
        ${JSON.stringify(chunk[badIndex], null, 2)}
        `
      );

      chunk.splice(badIndex, 1);
    }

    if (!success) {
      logError(
        `❌ Could not upload any product metafields in chunk for SKU ${product['Variant SKU']}`
      );
      allErrors.push(...chunk); // remaining chunk considered failed
    }
  }

  // Final log
  if (allErrors.length > 0) {
    logError(
      `Product metafield errors for SKU ${
        product['Variant SKU']
      }: ${JSON.stringify(allErrors)}`
    );
  } else {
    logError(
      `✅ Uploaded product metafields for SKU ${product['Variant SKU']}`
    );
  }
};

const uploadVariantMetafields = async (productId, index, variant) => {
  if (!variant.id) {
    logError(
      `❌ No variant ID found for SKU ${variant['Variant SKU']}`
    );
    return;
  }

  // All variant-level metafields according to new mapping
  const metafields = [
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'autoUpdatePrice',
      type: 'boolean',
      value:
        variant[
          'Auto Update Price (variant.metafields.variant.autoUpdatePrice)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'bangle_inner_diameter',
      type: 'number_decimal',
      value:
        variant[
          'Bangle Inner Diameter (variant.metafields.variant.bangle_inner_diameter)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'bangle_size',
      type: 'single_line_text_field',
      value:
        variant[
          'Bangle Size (variant.metafields.variant.bangle_size)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'bangle_bracelet_size_adjustable_up_to',
      type: 'single_line_text_field',
      value:
        variant[
          'Bangle/Bracelet Size Adjustable up-to (variant.metafields.variant.bangle_bracelet_size_adjustable_up_to)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'center_diamond_weight',
      type: 'number_decimal',
      value:
        variant[
          'Center Diamond Weight (variant.metafields.variant.center_diamond_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'certificate_1',
      type: 'single_line_text_field',
      value:
        variant[
          'Certificate # 1 (variant.metafields.variant.certificate_1)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'certificate_2',
      type: 'single_line_text_field',
      value:
        variant[
          'Certificate # 2 (variant.metafields.variant.certificate_2)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'certificate_3',
      type: 'single_line_text_field',
      value:
        variant[
          'Certificate # 3 (variant.metafields.variant.certificate_3)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'certificate_4',
      type: 'single_line_text_field',
      value:
        variant[
          'Certificate # 4 (variant.metafields.variant.certificate_4)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'certificate_5',
      type: 'single_line_text_field',
      value:
        variant[
          'Certificate # 5 (variant.metafields.variant.certificate_5)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'certification_type',
      type: 'single_line_text_field',
      value:
        variant[
          'Certification Type (variant.metafields.variant.certification_type)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'close_out',
      type: 'boolean',
      value:
        variant[
          'Close out (variant.metafields.variant.close_out)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'dc',
      type: 'single_line_text_field',
      value: variant['DC (variant.metafields.variant.dc)'] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'diamond_total_pcs',
      type: 'number_integer',
      value:
        variant[
          'Diamond Total Pcs (variant.metafields.variant.diamond_total_pcs)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'diamond_total_weight',
      type: 'number_decimal',
      value:
        variant[
          'Diamond Total Weight (variant.metafields.variant.diamond_total_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'entry_date',
      type: 'date',
      value:
        variant[
          'Entry Date (variant.metafields.variant.entry_date)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'gemstones_weight',
      type: 'number_decimal',
      value:
        variant[
          'Gemstones Weight (variant.metafields.variant.gemstones_weight)'
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
      key: 'length',
      type: 'number_decimal',
      value:
        variant[
          'Length (variant.metafields.variant.length)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'new_arrival',
      type: 'boolean',
      value:
        variant[
          'New Arrival (variant.metafields.variant.new_arrival)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'per_gram_or_disc',
      type: 'single_line_text_field',
      value:
        variant[
          'Per Gram Or Disc (variant.metafields.variant.per_gram_or_disc)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'price_fall',
      type: 'boolean',
      value:
        variant[
          'Price Fall (variant.metafields.variant.price_fall)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'ring_size',
      type: 'single_line_text_field',
      value:
        variant[
          'Ring Size (variant.metafields.variant.ring_size)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'show_tag_price',
      type: 'boolean',
      value:
        variant[
          'Show Tag Price (variant.metafields.variant.show_tag_price)'
        ]?.toString() || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'tag_price',
      type: 'money',
      value: JSON.stringify({
        amount:
          variant[
            'Tag Price (variant.metafields.variant.tag_price)'
          ]?.toFixed(2) || '0.00',
        currency_code: 'USD',
      }),
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'upload_date',
      type: 'date',
      value:
        variant[
          'Upload Date (variant.metafields.variant.upload_date)'
        ] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'vendor',
      type: 'single_line_text_field',
      value:
        variant['Vendor (variant.metafields.variant.vendor)'] || '',
    },
    {
      ownerId: variant.id,
      namespace: 'variant',
      key: 'vendorStyle',
      type: 'single_line_text_field',
      value:
        variant[
          'Vendor Style (variant.metafields.variant.vendorStyle)'
        ] || '',
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
          `✅ Uploaded ${chunk.length} variant metafields for SKU ${variant['Variant SKU']}`
        );
        break;
      }

      attempt++;
      logError(
        `⚠️ Attempt ${attempt}: ${userErrors.length} variant metafield errors`
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
          `🛑 Removed invalid variant metafield from chunk:\n` +
            `  → Key: ${removed.key}\n` +
            `  → Value: ${JSON.stringify(removed.value)}`
        );
      }
    }

    if (!success) {
      logError(
        `❌ Could not upload any variant metafields in chunk for SKU ${variant['Variant SKU']}`
      );
      allErrors.push(...chunk); // remaining chunk considered failed
    }
  }

  // Final log
  if (allErrors.length > 0) {
    logError(
      `Variant metafield errors for SKU ${
        variant['Variant SKU']
      }: ${JSON.stringify(allErrors)}`
    );
  } else {
    logError(
      `✅ Uploaded variant metafields for SKU ${variant['Variant SKU']}`
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
    '61-01704-EMR-Y-Y',
    '61-01715-EMR-Y-MNC',
    '61-00195-DP-Y-Y',
    '61-01234-EMR-Y-MNC',
    '61-00194-DP-Y-TT',
    '61-02304-EMR-Y-MNC',
    '61-01703-EMR-Y-MNC',
    '61-01707-EMR-Y-TT',
    '61-01712-EMR-Y-MNC',
    '61-01706-EMR-Y-TT',
    '61-01705-EMR-Y-Y',
    '61-01709-EMR-Y-MNC',
    '61-01710-EMR-Y-MNC',
    '61-01711-EMR-Y-MNC',
    '61-01233-EMR-Y-TT',
    '61-01714-EMR-Y-MNC',
    '61-01235-EMR-Y-MNC',
    '61-01230-EMR-Y-MNC',
    '70-01807-SCS-Y-TT',
    '70-00213-HPG-Y-Y',
    '70-00199-EMR-Y-TT',
    '70-01803-SCS-Y-TT',
    '70-00203-EMR-Y-TT',
    '70-00214-HPG-Y-TT',
    '70-00206-EMR-Y-TT',
    '70-00210-EMR-Y-MNC',
    '70-00208-EMR-Y-TT',
    '70-00215-EMR-Y-TT',
    '70-00205-EMR-Y-TT',
    '72-00227-DP-Y-Y',
    '72-00224-SCS-Y-TT',
    '72-00235-SCS-Y-TT',
    '72-00231-SCS-Y-TT',
    '72-00230-SCS-Y-TT',
    '72-01084-EMR-Y-TT',
    '72-01087-AU-Y-Y',
    '72-01092-AU-Y-Y',
    '72-01090-AU-Y-Y',
    '72-00219-DP-Y-TT',
    '72-01083-AU-Y-Y',
    '72-00225-DP-Y-TT',
    '72-00232-SCS-Y-TT',
    '72-00221-SCS-Y-TT',
    '72-01088-AU-Y-Y',
    '72-01085-AU-Y-TT',
    '72-01086-AU-Y-TT',
    '72-00220-SCS-Y-TT',
    '72-01818-DP-Y-Y',
    '72-00322-DP-Y-Y',
    '72-00320-DP-Y-Y',
    '72-02755-DP-Y-TT',
    '72-00323-DP-Y-Y',
    '72-00319-DP-Y-Y',
    '72-01492-EJZ-Y-Y',
    '72-02754-DP-Y-TT',
    '72-01820-DP-Y-Y',
    '72-01816-DP-Y-Y',
    '72-02757-DP-Y-TT',
    '72-01817-DP-Y-Y',
    '72-01819-DP-Y-Y',
    '72-02758-DP-Y-Y',
    '73-01145-EMR-Y-TT',
    '73-01129-EMR-Y-MNC',
    '73-00241-EMR-Y-TT',
    '73-00243-EMR-Y-TT',
    '73-01700-EMR-Y-Y',
    '73-01151-EMR-Y-Y',
    '73-00242-EMR-Y-TT',
    '73-01815-SCS-Y-TT',
    '73-01140-EMR-Y-TT',
    '73-01134-EMR-Y-Y',
    '73-01128-EMR-Y-TT',
    '73-01144-EMR-Y-TT',
    '73-01702-EMR-Y-Y',
    '73-01149-EMR-Y-Y',
    '73-01143-EMR-Y-TT',
    '73-01148-EMR-Y-TT',
    '73-01701-EMR-Y-Y',
    '73-01137-EMR-Y-TT',
    '73-01135-EMR-Y-TT',
    '73-01130-EMR-Y-TT',
    '73-01132-EMR-Y-MNC',
    '73-01141-EMR-Y-TT',
    '73-01147-EMR-Y-TT',
    '73-01150-EMR-Y-Y',
    '73-01138-EMR-Y-TT',
    '73-01152-EMR-Y-Y',
    '75-01636-EMR-Y-Y',
    '75-02275-EMR-Y-Y',
    '75-01647-EMR-Y-Y',
    '75-01676-EMR-Y-Y',
    '75-01638-EMR-Y-Y',
    '75-01219-EMR-Y-TT',
    '75-01217-EMR-Y-TT',
    '75-01662-EMR-Y-Y',
    '75-01169-EMR-Y-TT',
    '75-02256-EMR-Y-TT',
    '75-02469-EMR-Y-Y',
    '75-01615-EMR-Y-Y',
    '75-01685-EMR-R-R',
    '75-00116-EMR-Y-TT',
    '75-02274-EMR-Y-Y',
    '75-00256-EMR-Y-MNC',
    '75-01682-EMR-R-R',
    '75-01218-EMR-Y-TT',
    '75-01177-EMR-Y-Y',
    '75-01199-EMR-Y-Y',
    '75-01643-EMR-Y-Y',
    '75-02240-EMR-Y-TT',
    '75-01628-EMR-Y-MNC',
    '75-01668-EMR-Y-Y',
    '75-01225-EMR-Y-Y',
    '75-00263-EMR-Y-Y',
    '75-01673-EMR-Y-Y',
    '75-01210-EMR-Y-Y',
    '75-01204-EMR-Y-Y',
    '75-01652-EMR-Y-Y',
    '75-02230-EMR-Y-Y',
    '75-02244-EMR-Y-TT',
    '75-02255-EMR-Y-TT',
    '75-02231-EMR-Y-Y',
    '75-02219-EMR-Y-TT',
    '75-00180-EMR-Y-Y',
    '75-00251-EMR-Y-MNC',
    '75-01686-EMR-R-R',
    '75-00971-EMR-Y-TT',
    '75-01618-EMR-Y-Y',
    '75-02243-EMR-Y-TT',
    '75-01669-EMR-Y-Y',
    '75-02237-EMR-Y-TT',
    '75-01213-EMR-Y-TT',
    '75-02226-EMR-Y-TT',
    '75-00249-EMR-Y-MNC',
    '75-00264-EMR-Y-TT',
    '75-01167-EMR-Y-TT',
    '75-02245-EMR-Y-Y',
    '75-01175-EMR-Y-Y',
    '75-02241-EMR-Y-Y',
    '75-01193-EMR-Y-Y',
    '75-01187-EMR-Y-Y',
    '75-01616-EMR-Y-Y',
    '75-02220-EMR-Y-Y',
    '75-02221-EMR-Y-Y',
    '75-02218-EMR-Y-Y',
    '75-02265-EMR-Y-Y',
    '75-01206-EMR-Y-Y',
    '75-02238-EMR-Y-MNC',
    '75-02249-EMR-Y-Y',
    '75-01178-EMR-Y-Y',
    '75-02272-EMR-Y-TT',
    '75-01202-EMR-Y-TT',
    '75-01631-EMR-Y-Y',
    '75-02074-SON-Y-TT',
    '75-01680-EMR-Y-Y',
    '75-02262-EMR-Y-Y',
    '75-01168-EMR-Y-TT',
    '75-01623-EMR-Y-Y',
    '75-02468-EMR-Y-Y',
    '75-00972-EMR-Y-Y',
    '75-02227-EMR-Y-TT',
    '75-01653-EMR-Y-Y',
    '75-01214-EMR-Y-Y',
    '75-01627-EMR-Y-MNC',
    '75-01655-EMR-Y-Y',
    '75-01670-EMR-Y-Y',
    '75-01629-EMR-Y-MNC',
    '75-02222-EMR-Y-Y',
    '75-01639-EMR-Y-Y',
    '75-01182-EMR-Y-TT',
    '75-01154-EMR-Y-TT',
    '75-01656-EMR-Y-Y',
    '75-01660-EMR-Y-TT',
    '75-02232-EMR-Y-Y',
    '75-01648-EMR-Y-Y',
    '75-01649-EMR-Y-Y',
    '75-02252-EMR-Y-Y',
    '75-01632-EMR-Y-Y',
    '75-01645-EMR-Y-Y',
    '75-01227-EMR-Y-Y',
    '75-01180-EMR-Y-MNC',
    '75-01228-EMR-Y-Y',
    '75-00971-EMR-Y-Y',
    '75-02235-EMR-Y-Y',
    '75-02253-EMR-Y-Y',
    '75-02228-EMR-Y-Y',
    '75-01208-EMR-Y-Y',
    '75-01222-EMR-Y-TT',
    '75-00973-EMR-Y-Y',
    '75-02072-SON-Y-Y',
    '75-02076-SON-Y-TT',
    '75-01640-EMR-Y-Y',
    '75-01619-EMR-Y-TT',
    '75-01173-EMR-Y-Y',
    '75-01659-EMR-Y-Y',
    '75-01161-EMR-Y-TT',
    '75-01190-EMR-Y-Y',
    '75-01181-EMR-Y-MNC',
    '75-01633-EMR-Y-Y',
    '75-01188-EMR-Y-Y',
    '75-02258-EMR-Y-Y',
    '75-01663-EMR-Y-TT',
    '75-01634-EMR-Y-Y',
    '75-01192-EMR-Y-Y',
    '75-01159-EMR-Y-TT',
    '75-01229-EMR-Y-TT',
    '75-02271-EMR-Y-Y',
    '75-02254-EMR-Y-TT',
    '75-02257-EMR-Y-Y',
    '75-01675-EMR-Y-Y',
    '75-01667-EMR-Y-Y',
    '75-01221-EMR-Y-TT',
    '75-02464-EMR-Y-TT',
    '75-01641-EMR-Y-Y',
    '75-02467-EMR-Y-MNC',
    '75-01157-EMR-Y-TT',
    '75-01364-EMR-Y-TT',
    '75-01166-EMR-Y-Y',
    '75-01624-EMR-Y-Y',
    '75-01651-EMR-Y-Y',
    '75-01160-EMR-Y-Y',
    '75-01201-EMR-Y-TT',
    '75-01212-EMR-Y-Y',
    '75-02071-SON-Y-Y',
    '75-01171-EMR-Y-Y',
    '75-01176-EMR-Y-TT',
    '75-01172-EMR-Y-TT',
    '75-01672-EMR-Y-Y',
    '75-01174-EMR-Y-Y',
    '75-02465-EMR-Y-TT',
    '75-01679-EMR-Y-MNC',
    '75-02075-SON-Y-TT',
    '75-00252-EMR-Y-MNC',
    '75-00248-EMR-Y-TT',
    '75-01170-EMR-Y-TT',
    '78-01124-EMR-Y-MNC',
    '78-00001-EMR-Y-Y',
    '78-01126-EMR-Y-TT',
    '78-01116-EMR-Y-Y',
    '78-01499-EMR-Y-MNC',
    '78-01105-EMR-Y-Y',
    '78-01811-SCS-Y-TT',
    '78-01498-EMR-Y-Y',
    '78-01125-EMR-Y-Y',
    '78-00772-EMR-Y-MNC',
    '78-01813-SCS-Y-MNC',
    '78-01502-EMR-Y-Y',
    '78-02283-EMR-Y-TT',
    '78-00324-EMR-Y-TT',
    '78-01110-EMR-Y-Y',
    '78-00004-EMR-Y-Y',
    '78-01497-EMR-Y-Y',
    '78-00769-EMR-Y-Y',
    '78-00003-EMR-Y-TT',
    '78-02279-EMR-Y-Y',
    '78-00015-EMR-Y-Y',
    '78-00770-EMR-Y-Y',
    '78-01496-EMR-Y-TT',
    '78-00016-EMR-Y-NA',
    '78-00006-EMR-Y-MNC',
    '78-00277-EMR-Y-Y',
    '78-00278-EMR-Y-TT',
    '78-00002-EMR-Y-MNC',
    '78-00773-EMR-Y-TT',
    '78-00279-EMR-Y-Y',
    '78-01127-EMR-Y-Y',
    '78-01119-EMR-Y-TT',
    '78-01113-EMR-Y-TT',
    '78-00007-EMR-Y-Y',
    '78-01112-EMR-Y-TT',
    '78-00273-EMR-Y-MNC',
    '78-00015-EMR-Y-NA',
    '78-01123-EMR-Y-Y',
    '78-01501-EMR-Y-MNC',
    '78-01095-EMR-Y-TT',
    '78-00011-EMR-Y-Y',
    '78-00014-EMR-Y-NA',
    '78-00005-EMR-Y-Y',
    '78-00275-EMR-Y-Y',
    '78-01121-EMR-Y-Y',
    '78-01120-EMR-Y-TT',
    '78-01097-EMR-Y-TT',
    '78-01118-EMR-Y-TT',
    '78-00276-EMR-Y-Y',
    '78-01096-EMR-Y-Y',
    '78-00268-EMR-Y-TT',
    '78-01102-EMR-Y-TT',
    '78-01107-EMR-Y-Y',
    '78-01115-EMR-Y-TT',
    '78-01101-EMR-Y-TT',
    '78-01114-EMR-Y-Y',
    '78-01103-EMR-Y-Y',
    '78-01104-EMR-Y-TT',
    '78-01109-EMR-Y-TT',
    '78-01099-EMR-Y-TT',
    '78-01094-EMR-Y-TT',
    '78-00272-EMR-Y-TT',
    '78-01106-EMR-Y-Y',
    '400-01422-KPS-R-R',
    '400-01994-GS-R-R',
    '400-01984-GS-Y-Y',
    '400-01430-KPS-Y-Y',
    '400-01996-GS-Y-Y',
    '400-01400-KPS-Y-Y',
    '400-01428-KPS-R-R',
    '400-01998-GS-Y-Y',
    '400-01987-GS-Y-Y',
    '400-01986-GS-Y-Y',
    '400-00499-GS-R-R',
    '400-01989-GS-Y-Y',
    '400-01983-GS-Y-Y',
    '400-01988-GS-Y-Y',
    '400-01393-KPS-Y-Y',
    '400-01995-GS-Y-Y',
    '400-01399-KPS-Y-Y',
    '400-01398-KPS-Y-Y',
    '400-01982-GS-Y-Y',
    '400-00501-GS-R-R',
    '400-00500-GS-Y-Y',
    '401-00516-GS-R-R',
    '401-00519-GS-R-R',
    '401-01446-KPS-Y-Y',
    '401-01445-KPS-Y-Y',
    '401-00502-GS-Y-Y',
    '401-00507-SHT-R-R',
    '401-00518-GS-Y-Y',
    '401-00515-GS-R-R',
    '401-00505-KS-Y-Y',
    '401-01444-KPS-Y-Y',
    '401-00513-GS-R-R',
    '401-00509-GS-Y-Y',
    '401-00503-GS-Y-Y',
    '401-00510-GS-R-R',
    '401-00512-GS-Y-Y',
    '402-00530-GS-Y-Y',
    '402-00531-GS-Y-Y',
    '402-00523-GS-R-RW',
    '402-00524-GS-R-R',
    '402-00532-GS-R-R',
    '402-00525-GS-Y-Y',
    '402-00528-GS-Y-Y',
    '402-00529-GS-R-R',
    '410-00537-GS-W-W',
    '411-00544-GS-W-W',
    '411-00547-GS-W-W',
    '411-00548-GS-W-W',
    '411-00538-CF-W-W',
    '411-00546-GS-W-W',
    '411-00542-GS-W-W',
    '411-00545-GS-W-W',
    '411-00543-GS-W-W',
    '412-00554-GS-W-W',
    '412-00549-GS-W-W',
    '412-00555-GS-W-W',
    '412-00551-GS-W-W',
    '412-00552-GS-W-W',
    '412-00553-GS-W-W',
  ]; // sequential input, one at a time

  for (const code of multiStyleCodes) {
    logError(`Processing MultiStyleCode: ${code}`);
    const variants = await getDataByMultiStyleCode(code);

    if (!variants.length) {
      logError(`No records found for MultiStyleCode: ${code}`);
      continue;
    }

    await uploadGroupedProduct(variants, code);
    console.log(`Finished MultiStyleCode: ${code}`);
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
