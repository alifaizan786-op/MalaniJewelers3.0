Use MalaniLive

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
    CASE WHEN Styles.AttribField104 = 'Yes' THEN 'TRUE' ELSE 'FALSE' END AS 'Chain included in the price (product.metafields.sku.chain_included_in_the_price)',
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
        Styles.AttribField141 IS NULL AND 
        Styles.MultiSkuCode IS NULL AND 
        Styles.isDeleted IS NULL AND
        ClassCodes.ClassCode = '21'