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
          variants(first: 1) {
            nodes {
              id
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

const getData = async (ClassCode) => {
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
            Styles.SKUCode
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
    CASE WHEN Styles.AutoUpdatePrice = 1 THEN 'TRUE' ELSE 'FALSE' END AS 'Auto Update Price (variant.metafields.variant.autoUpdatePrice)'

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
        Styles.AttribField141 IS NULL AND 
        Styles.MultiSkuCode IS NULL AND 
        Styles.isDeleted IS NULL AND
        ClassCodes.ClassCode = '${ClassCode}'

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

const uploadProductWithMetafields = async (product) => {
  const input = {
    title: product.Title,
    bodyHtml: product['Body (HTML)'],
    vendor: product.Vendor,
    handle: product.Handle,
    published: true,
    productType: product.Type || 'Jewelry',
    productCategory: {
      productTaxonomyNodeId: 'gid://shopify/ProductTaxonomyNode/340',
    },
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

    variants: [
      {
        sku: product['Variant SKU'],
        price: product['Variant Price']?.toString() || '0',
        compareAtPrice:
          product['Variant Compare At Price']?.toString() || null,
        inventoryManagement: 'SHOPIFY',
        inventoryPolicy: 'DENY',
        taxable: true,
        requiresShipping: true,
        weight: Number(product['Variant Grams']) || 0,
        weightUnit: 'GRAMS',
      },
    ],
  };

  const productCreateRes = await graphQL(createProductMutation, {
    input,
  });

  const productId =
    productCreateRes?.data?.productCreate?.product?.id;
  const variantId =
    productCreateRes?.data?.productCreate?.product?.variants
      ?.nodes?.[0]?.id;
  const userErrors =
    productCreateRes?.data?.productCreate?.userErrors;

  if (!productId || !variantId) {
    logError(
      `Failed to create product for SKU ${
        product['Variant SKU']
      }. Errors: ${JSON.stringify(userErrors)}`
    );
    return;
  } else {
    logError(
      `Successfully created a product for ${product['Variant SKU']}. ProductId: ${productId}, VariantId: ${variantId}`
    );
  }

  const imageData = await getImageData(product.Code);
  const imageArr = imageData.map(
    (img) =>
      `https://www.malanijewelers.com/TransactionImages/Styles/large/${img.LargeImg}`
  );

  // Attach images
  await attachImagesToProduct(
    productId,
    imageArr,
    product['Variant SKU']
  );

  // Prepare PRODUCT metafields
  const productMetafields = [
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
              .split(/\s{2,}/)
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
        product['metal type (product.metafields.sku.metal_type)'] ||
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

  // Prepare VARIANT metafields
  const variantMetafields = [
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'autoUpdatePrice',
      type: 'boolean',
      value:
        product[
          'Auto Update Price (variant.metafields.variant.autoUpdatePrice)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'bangle_inner_diameter',
      type: 'number_decimal',
      value:
        product[
          'Bangle Inner Diameter (variant.metafields.variant.bangle_inner_diameter)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'bangle_size',
      type: 'single_line_text_field',
      value:
        product[
          'Bangle Size (variant.metafields.variant.bangle_size)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'bangle_bracelet_size_adjustable_up_to',
      type: 'single_line_text_field',
      value:
        product[
          'Bangle/Bracelet Size Adjustable up-to (variant.metafields.variant.bangle_bracelet_size_adjustable_up_to)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'center_diamond_weight',
      type: 'number_decimal',
      value:
        product[
          'Center Diamond Weight (variant.metafields.variant.center_diamond_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'certificate_1',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 1 (variant.metafields.variant.certificate_1)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'certificate_2',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 2 (variant.metafields.variant.certificate_2)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'certificate_3',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 3 (variant.metafields.variant.certificate_3)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'certificate_4',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 4 (variant.metafields.variant.certificate_4)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'certificate_5',
      type: 'single_line_text_field',
      value:
        product[
          'Certificate # 5 (variant.metafields.variant.certificate_5)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'certification_type',
      type: 'single_line_text_field',
      value:
        product[
          'Certification Type (variant.metafields.variant.certification_type)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'close_out',
      type: 'boolean',
      value:
        product[
          'Close out (variant.metafields.variant.close_out)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'dc',
      type: 'single_line_text_field',
      value: product['DC (variant.metafields.variant.dc)'] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'diamond_total_pcs',
      type: 'number_integer',
      value:
        product[
          'Diamond Total Pcs (variant.metafields.variant.diamond_total_pcs)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'diamond_total_weight',
      type: 'number_decimal',
      value:
        product[
          'Diamond Total Weight (variant.metafields.variant.diamond_total_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'entry_date',
      type: 'date',
      value:
        product[
          'Entry Date (variant.metafields.variant.entry_date)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'gemstones_weight',
      type: 'number_decimal',
      value:
        product[
          'Gemstones Weight (variant.metafields.variant.gemstones_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'gross_weight',
      type: 'number_decimal',
      value:
        product[
          'Gross Weight (variant.metafields.variant.gross_weight)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'length',
      type: 'number_decimal',
      value:
        product[
          'Length (variant.metafields.variant.length)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'new_arrival',
      type: 'boolean',
      value:
        product[
          'New Arrival (variant.metafields.variant.new_arrival)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'per_gram_or_disc',
      type: 'single_line_text_field',
      value:
        product[
          'Per Gram Or Disc (variant.metafields.variant.per_gram_or_disc)'
        ] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'price_fall',
      type: 'boolean',
      value:
        product[
          'Price Fall (variant.metafields.variant.price_fall)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'ring_size',
      type: 'single_line_text_field',
      value:
        product['Ring Size (variant.metafields.variant.ring_size)'] ||
        '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'show_tag_price',
      type: 'boolean',
      value:
        product[
          'Show Tag Price (variant.metafields.variant.show_tag_price)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'tag_price',
      type: 'money',
      value: JSON.stringify({
        amount:
          product[
            'Tag Price (variant.metafields.variant.tag_price)'
          ]?.toFixed(2) || '0.00',
        currency_code: 'USD',
      }),
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'upload_date',
      type: 'date',
      value:
        product[
          'Upload Date (variant.metafields.variant.upload_date)'
        ]?.toString() || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'vendor',
      type: 'single_line_text_field',
      value:
        product['Vendor (variant.metafields.variant.vendor)'] || '',
    },
    {
      ownerId: variantId,
      namespace: 'variant',
      key: 'vendorStyle',
      type: 'single_line_text_field',
      value:
        product[
          'Vendor Style (variant.metafields.variant.vendorStyle)'
        ] || '',
    },
  ];

  const validMetafields = [
    ...productMetafields,
    ...variantMetafields,
  ].filter(
    (m) => m.value !== null && m.value !== undefined && m.value !== ''
  );

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

      console.dir(userErrors, { depth: null });

      for (let i = 0; i < userErrors.length; i++) {
        const element = userErrors[i];

        if (element.field) {
          console.dir(chunk[parseInt(element.field[1])], {
            depth: null,
          });
        }
      }

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

const attachImagesToProduct = async (productId, imageUrls, sku) => {
  if (!imageUrls?.length) return;

  const media = imageUrls.map((src) => ({
    originalSource: src,
    mediaContentType: 'IMAGE',
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
  const classCodes = [
    '61',
    '69',
    '70',
    '72',
    '73',
    '75',
    '77',
    '78',
    '236',
    '390',
    '391',
    '392',
    '393',
    '395',
    '398',
    '399',
    '400',
    '401',
    '402',
    '403',
    '404',
    '405',
    '406',
    '410',
    '411',
    '412',
    '413',
    '414',
    '415',
    '416',
    '420',
    '421',
    '422',
    '423',
    '424',
    '425',
  ]; // add as many as you want here

  for (const classCode of classCodes) {
    logError(
      `\n=== 📦 Starting upload for ClassCode: ${classCode} ===`
    );

    const result = await getData(classCode);

    for (const product of result) {
      try {
        await uploadProductWithMetafields(product);
      } catch (err) {
        logError(
          `❌ Exception while processing SKU ${product['Variant SKU']} (ClassCode ${classCode}): ${err.message}`
        );
      }
    }

    logError(
      `✅ Finished ClassCode ${classCode} — Processed ${result.length} products\n`
    );
  }

  console.log('🎉 All ClassCodes finished.');
})();

function toProperCase(str) {
  if (!str || typeof str !== 'string') return '';
  return str.replace(/\w\S*/g, (txt) => {
    return txt.charAt(0).toUpperCase() + txt.slice(1).toLowerCase();
  });
}
