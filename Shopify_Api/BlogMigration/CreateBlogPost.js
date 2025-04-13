require('dotenv').config();
const axios = require('axios');
const allBlogs = require('./allBlogs.json'); // Update path if needed

const shop = process.env.Shopify_Shop_Name;
const accessToken = process.env.Shopify_Admin_Api_Access_Token;

// Optional utility to create slugs if needed later
const slugify = (str) =>
  str
    .toLowerCase()
    .replace(/[^\w ]+/g, '')
    .replace(/ +/g, '-');

async function createArticle(
  blogId,
  title,
  bodyHtml,
  handle,
  author,
  tags,
  excerpt,
  excerptHtml,
  published,
  publishedAt,
  imageSrc,
  seoTitle,
  seoDescription
) {
  try {
    const url = `${shop}/admin/api/2024-01/blogs/${blogId}/articles.json`;

    const articleData = {
      title,
      body_html: bodyHtml,
      handle,
      author,
      tags,
      excerpt,
      excerpt_html: excerptHtml,
      published,
      published_at: publishedAt,
      seo: {
        title: seoTitle,
        description: seoDescription,
      },
    };

    if (imageSrc) {
      articleData.image = { src: imageSrc };
    }

    const response = await axios.post(
      url,
      { article: articleData },
      {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json',
        },
      }
    );

    console.log('✅ Article Created:', response.data.article.title);
  } catch (error) {
    console.error(
      '❌ Error creating article:',
      error.response?.data || error.message
    );
  }
}

(async () => {
  for (let i = 0; i < allBlogs.length; i++) {
    const blog = allBlogs[i];

    // Sanitize publishedAt to ISO 8601 format if present
    let publishedAtISO = null;
    if (blog.publishedAt) {
      const parsedDate = new Date(
        blog.publishedAt.replace(/:([0-9]{2})$/, '.$1Z')
      );
      if (!isNaN(parsedDate)) {
        publishedAtISO = parsedDate.toISOString();
      } else {
        console.warn(
          `⚠️ Skipping invalid publishedAt: ${blog.publishedAt}`
        );
      }
    }

    console.log(
      `🚀 Creating blog ${i + 1}/${allBlogs.length}: ${blog.title}`
    );

    await createArticle(
      blog.blogId,
      blog.title,
      blog.bodyhtml,
      blog.handle || slugify(blog.title),
      blog.author || 'Malani Jewelers',
      blog.tags || '',
      blog.excerpt || '',
      null, // excerpt_html is not provided
      blog.published ?? true,
      publishedAtISO,
      blog.ImageSrc || '',
      blog.seoTitle || blog.title,
      blog.seoDescription || blog.excerpt
    );
  }
})();
