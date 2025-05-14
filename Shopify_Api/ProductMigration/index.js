const sql = require('mssql');
require('dotenv').config();
const axios = require('axios');
const shop = process.env.Shopify_Shop_Name;
const accessToken = process.env.Shopify_Admin_Api_Access_Token;

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

const getData = async () => {
  const db = await sql.connect(connection);
  const query = `
    SELECT TOP 10 
    Styles.Code,
    Styles.SKUCode,
    Styles.StyleDesc,
    Styles.SubCatCode,
     CONCAT(
        CASE WHEN CSC3.CatSubCat IS NOT NULL THEN CSC3.CatSubCat ELSE '' END,
        CASE WHEN CSC3.CatSubCat IS NOT NULL THEN '->' ELSE '' END,
        CASE WHEN CSC2.CatSubCat IS NOT NULL THEN CSC2.CatSubCat ELSE '' END,
        CASE WHEN CSC2.CatSubCat IS NOT NULL AND CatSubCats.CatSubCat IS NOT NULL THEN '->' ELSE '' END,
        COALESCE(CatSubCats.CatSubCat, '')
    ) AS CategoryHierarchy, -- Directly added to SELECT
    GoldKarat.LongData AS GoldKarat,
    Styles.AttribField84 AS Finish,
    Styles.AttribField85 AS NumberOfPcs,
    Styles.AttribField86 AS JewelryFor,
    Styles.AttribField115 AS JewelryType,
    CatSubCats.CatSubCat AS Category,
    Styles.GoldWeight,
    Styles.AttribField87 AS CenterDiamondWeight,
    Styles.AttribField88 AS DiamondTotalWeight,
    Styles.AttribField89 AS DiamondTotalPcs,
    Styles.AttribField90 AS DiamondClarity,
    Styles.AttribField91 AS DiamondColor,
    Styles.AttribField92 AS Length,
    Styles.AttribField93 AS Width,
    Styles.AttribField83 AS PendantLength,
    Styles.AttribField94 AS PendantWidth,
    Styles.AttribField95 AS EarringLength,
    Styles.AttribField96 AS EarringWidth,
    Styles.AttribField105 AS EarringPostType,
    Styles.StyleEntryDate,
    ClassCodes.ClassCode,
    Styles.VendStyleCode,
    Styles.StyleGrossWt,
    Styles.TagPrice,
    Styles.StockQty,
    Styles.IsCloseOut,
    Styles.IsNewArrived,
    Styles.IsHotSeller,
    Styles.StyleStory,
    Styles.StoreCode,
    Styles.ShowPriceFallFlag,
    Styles.CustPrice,
    Styles.PerGramOrDisc,
    Styles.Minorcode,
    Styles.Purchasable,
    Styles.StyleUploadDate,
    Styles.Hidden,
    Styles.IsGIACertified,
    Styles.AutoUpdatePrice,
    Styles.ShowRetailPrice,
    Styles.StyleLongDesc,
    Styles.AttribField97 AS RingSize,
    Styles.AttribField98 AS RingDesignHeight,
    Styles.AttribField99 AS RingWidth,
    Styles.AttribField100 AS BangleSize,
    Styles.AttribField101 AS BangleInnerDiameter,
    Styles.AttribField102 AS BangleWidth, 
    Styles.AttribField103 AS BangleDesignHeight,
    Styles.AttribField104 AS ChainIncludedInPrice,
    Styles.AttribField106 AS BangleBraceletType,
    Styles.AttribField107 AS NosePinType,
    Styles.AttribField108 AS RingType,
    Styles.AttribField109 AS WatchDisclaimer,
    Styles.AttribField110 AS ChnageableStoneIncluded,
    Styles.AttribField111 AS BangleBraceletSizeAdjustableUpTo,
    Styles.AttribField112 AS DiamondType,
    Styles.AttribField113 AS GemstoneType,
    Styles.AttribField114 AS GemstoneWeight,
    Styles.AttribField117 AS ChainLength,
    Styles.AttribField118 AS CertNo,
    Styles.AttribField119 AS Cert,
    Styles.AttribField120 AS DC,
    Styles.IsCustomDesign,
    Styles.AttribField122 AS KW1,
    Styles.AttribField123 AS KW2,
    Styles.AttribField124 AS KW3,
    Styles.AttribField125 AS KW4,
    Styles.AttribField126 AS KW5,
    Styles.AttribField127 AS KW6,
    Styles.AttribField128 AS KW7,
    Styles.AttribField129 AS KW18,
    Styles.AttribField131 AS SearchUploadDate,
    Styles.AttribField132 AS Cert#2,
    Styles.AttribField133 AS Cert#3,
    Styles.AttribField134 AS Cert#2,
    Styles.AttribField141 AS MultiStyleCode,
    Styles.AttribField142 AS WhyHidden,
    Vendors.VendorName AS Vendor,
    Color.LongData AS Color
FROM 
    Styles
LEFT JOIN 
    CatSubCats ON Styles.SubCatCode = CatSubCats.Code -- Level 1
LEFT JOIN 
    CatSubCats AS CSC2 ON CatSubCats.ParentCode = CSC2.Code -- Level 2
LEFT JOIN 
    CatSubCats AS CSC3 ON CSC2.ParentCode = CSC3.Code -- Level 3
LEFT JOIN 
    Vendors ON Styles.VendCode = Vendors.Code
LEFT JOIN 
    ClassCodes ON Styles.ClassCode = ClassCodes.Code
LEFT JOIN 
    CommonMastersData AS GoldKarat ON Styles.GoldKt = GoldKarat.Code
LEFT JOIN 
    CommonMastersData AS Color ON Styles.Color = Color.Code
AND
    Styles.StockQty = 1
AND 
    Styles.Hidden = 0
AND 
    Styles.Purchasable = 1
  `;
  const result = await db.request().query(query);
  return result.recordset;
};

// Use async function to log properly
(async () => {
  const data = await getData();
  console.log(data);
})();
