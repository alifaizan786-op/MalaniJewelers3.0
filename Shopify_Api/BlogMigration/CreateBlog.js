require('dotenv').config();
const axios = require('axios');

const shop = process.env.Shopify_Shop_Name;
const accessToken = process.env.Shopify_Admin_Api_Access_Token;

async function createBlog(
  title,
  handle,
  blogBody,
  commentable = 'moderate',
  tags = ''
) {
  try {
    const url = `${shop}/admin/api/2024-01/blogs.json`;
    const response = await axios.post(
      url,
      {
        blog: {
          title,
          handle,
          body_html: blogBody,
          // commentable,
          tags,
        },
      },
      {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json',
        },
      }
    );

    console.log(
      '✅ Blog Created:',
      `${response.data.blog.title} - ${response.data.blog.id}`
    );
  } catch (error) {
    console.error(
      '❌ Error creating blog:',
      error.response?.data || error.message
    );
  }
}

// Example usage
// createBlog(
//   'Jewelry Education & Care',
//   'jewelry-education-and-care',
//   '<p>Learn how to care for your jewelry, understand diamond quality, and explore expert guides that help you invest in timeless elegance.</p>',
//   'moderate',
//   'jewelry care, diamond education, how to clean jewelry, gemstone guide, jewelry tips'
// );
// Note: Make sure to replace the `title`, `handle`, and `blogBody` with your desired values. The `commentable` field can be set to 'true', 'false', or 'moderate'. The `tags` field is optional and can be used to categorize your blog posts.

const blogs = [
  {
    title: 'Jewelry Education & Care',
    handle: 'jewelry-education-and-care',
    shortBodyHtml:
      '<p>Learn how to care for your jewelry, understand diamond quality, and explore expert guides that help you invest in timeless elegance.</p>',
    tags: 'jewelry care, diamond education, how to clean jewelry, gemstone guide, jewelry tips',
  },
  {
    title: 'Malani News & Highlights',
    handle: 'malani-news-and-highlights',
    shortBodyHtml:
      '<p>Stay up-to-date with the latest store updates, events, new arrivals, and everything happening at Malani Jewelers.</p>',
    tags: 'malani jewelers news, store updates, jewelry trends, malani events, press releases',
  },
  {
    title: 'Collections & Lookbooks',
    handle: 'collections-and-lookbooks',
    shortBodyHtml:
      '<p>Discover curated jewelry collections and seasonal lookbooks crafted to inspire your next timeless look.</p>',
    tags: 'jewelry collections, lookbooks, bridal jewelry, seasonal styles, new arrivals',
  },
  {
    title: 'Styling Tips & Guides',
    handle: 'styling-tips-and-guides',
    shortBodyHtml:
      '<p>Master the art of accessorizing with expert jewelry styling tips, pairing ideas, and trend-forward guides.</p>',
    tags: 'styling tips, jewelry fashion, how to wear jewelry, accessory guide, jewelry trends',
  },
  {
    title: 'Festivals & Holidays - Western Holidays',
    handle: 'western-holiday-jewelry',
    shortBodyHtml:
      '<p>Find the perfect jewelry gifts and styles to celebrate Christmas, Thanksgiving, New Year’s, and more.</p>',
    tags: 'holiday gifts, christmas jewelry, thanksgiving jewelry, western holidays, festive styles',
  },
  {
    title: 'Love & Relationships',
    handle: 'love-and-relationships',
    shortBodyHtml:
      '<p>Discover romantic gift ideas and jewelry that speaks from the heart—for anniversaries, engagements, and more.</p>',
    tags: 'love jewelry, anniversary gifts, couple jewelry, engagement, relationship gifts',
  },
  {
    title: 'Festivals & Holidays - Indian Festivals',
    handle: 'indian-festival-jewelry',
    shortBodyHtml:
      '<p>Celebrate Diwali, Raksha Bandhan, Karva Chauth, and more with traditional and modern jewelry styles.</p>',
    tags: 'diwali jewelry, indian festivals, traditional jewelry, festive gifts, raksha bandhan',
  },
  {
    title: 'Kids’ Jewelry',
    handle: 'kids-jewelry',
    shortBodyHtml:
      '<p>Shop safe, stylish, and playful jewelry designed just for kids—perfect for birthdays, milestones, and celebrations.</p>',
    tags: "kids jewelry, baby gifts, children's earrings, gold for kids, cute jewelry",
  },
  {
    title: 'Mother’s Day',
    handle: 'mothers-day-jewelry',
    shortBodyHtml:
      '<p>Honor the mother figures in your life with meaningful, sparkling gifts that celebrate their love and strength.</p>',
    tags: "mother's day, mom gifts, jewelry for mom, meaningful gifts, gold for mom",
  },
  {
    title: 'Father’s Day',
    handle: 'fathers-day-jewelry',
    shortBodyHtml:
      '<p>Explore unique and stylish jewelry gifts for dads—rings, bracelets, and chains that embody strength and love.</p>',
    tags: "father's day, jewelry for dad, gifts for him, men’s gold, masculine jewelry",
  },
  {
    title: 'Occasion-Based Jewelry',
    handle: 'occasion-based-jewelry',
    shortBodyHtml:
      '<p>Celebrate life’s moments with jewelry crafted for weddings, graduations, birthdays, and everything in between.</p>',
    tags: 'wedding jewelry, birthday gifts, graduation jewelry, special occasion, milestone gifts',
  },
  {
    title: 'Sales & Shopping Events',
    handle: 'sales-and-shopping-events',
    shortBodyHtml:
      '<p>Don’t miss our exclusive sales, seasonal offers, and event-based shopping specials at Malani Jewelers.</p>',
    tags: 'jewelry sale, shopping events, discounts, promotions, limited time offers',
  },
  {
    title: 'Men’s Jewelry',
    handle: 'mens-jewelry',
    shortBodyHtml:
      '<p>Explore sophisticated and powerful jewelry styles for men—from gold chains and rings to bracelets and pendants.</p>',
    tags: 'men’s jewelry, gold chains, rings for men, masculine style, jewelry for him',
  },
];

for (let i = 0; i < blogs.length; i++) {
  const { title, handle, shortBodyHtml, tags } = blogs[i];
  createBlog(title, handle, shortBodyHtml, 'moderate', tags);
}
