import express from 'express';
import { v4 as uuidv4 } from 'uuid';
import { pool } from '../db/init.js';
import { JSDOM } from 'jsdom';
import createDOMPurify from 'dompurify';
import rateLimit from 'express-rate-limit';

const router = express.Router();

// Initialize DOMPurify with custom config
const window = new JSDOM('').window;
const DOMPurify = createDOMPurify(window);

// Simple in-memory cache implementation
const cache = {
  posts: new Map(),
  categories: new Map(),
  featuredPosts: new Map(),
  trendingPosts: new Map(),
  TTL: 5 * 60 * 1000, // 5 minutes cache TTL
};

// Cache cleanup interval
setInterval(() => {
  const now = Date.now();
  for (const [key, value] of cache.posts.entries()) {
    if (now > value.timestamp + cache.TTL) {
      cache.posts.delete(key);
    }
  }
  for (const [key, value] of cache.categories.entries()) {
    if (now > value.timestamp + cache.TTL) {
      cache.categories.delete(key);
    }
  }
  for (const [key, value] of cache.featuredPosts.entries()) {
    if (now > value.timestamp + cache.TTL) {
      cache.featuredPosts.delete(key);
    }
  }
  for (const [key, value] of cache.trendingPosts.entries()) {
    if (now > value.timestamp + cache.TTL) {
      cache.trendingPosts.delete(key);
    }
  }
}, 60000); // Clean up every minute

// Function to generate meta tags HTML
function generateMetaTags(post) {
  return `
    <!-- Primary Meta Tags -->
    <title>${post.meta_title || post.title} | Boomlify Blog</title>
    <meta name="title" content="${post.meta_title || post.title}">
    <meta name="description" content="${post.meta_description}">
    
    <!-- Open Graph / Facebook -->
    <meta property="og:type" content="${post.og_type || 'article'}">
    <meta property="og:url" content="${post.canonical_url}">
    <meta property="og:title" content="${post.og_title || post.title}">
    <meta property="og:description" content="${post.og_description || post.meta_description}">
    <meta property="og:image" content="${post.og_image || post.featured_image}">
    
    <!-- Twitter -->
    <meta property="twitter:card" content="${post.twitter_card || 'summary_large_image'}">
    <meta property="twitter:url" content="${post.canonical_url}">
    <meta property="twitter:title" content="${post.twitter_title || post.title}">
    <meta property="twitter:description" content="${post.twitter_description || post.meta_description}">
    <meta property="twitter:image" content="${post.twitter_image || post.featured_image}">
    
    <!-- Canonical URL -->
    <link rel="canonical" href="${post.canonical_url}">
    
    <!-- Structured Data -->
    <script type="application/ld+json">
      ${JSON.stringify({
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": post.title,
        "description": post.meta_description,
        "image": post.featured_image,
        "author": {
          "@type": "Person",
          "name": post.author || "Boomlify Team"
        },
        "publisher": {
          "@type": "Organization",
          "name": "Boomlify",
          "logo": {
            "@type": "ImageObject",
            "url": "https://boomlify.com/logo.png"
          }
        },
        "datePublished": post.created_at,
        "dateModified": post.updated_at,
        "mainEntityOfPage": {
          "@type": "WebPage",
          "@id": post.canonical_url
        },
        "keywords": post.keywords,
        "articleBody": post.content.replace(/<[^>]*>/g, ''),
        "wordCount": post.content.split(/\s+/).length
      })}
    </script>
  `;
}

// Function to validate YouTube URLs
const isValidYouTubeUrl = (url) => {
  return url.match(/^https?:\/\/(www\.)?youtube\.com\/embed\/[a-zA-Z0-9_-]+/);
};

// Configure DOMPurify to allow iframes from trusted sources
DOMPurify.setConfig({
  ADD_TAGS: ['iframe'],
  ADD_ATTR: ['allow', 'allowfullscreen', 'frameborder', 'scrolling', 'src', 'title', 'width', 'height'],
  ALLOWED_URI_REGEXP: /^(?:(?:(?:f|ht)tps?|mailto|tel|callto|cid|xmpp|xxx):|[^a-z]|[a-z+.\-]+(?:[^a-z+.\-:]|$))/i,
  ALLOWED_TAGS: [
    'a', 'b', 'br', 'div', 'em', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
    'i', 'iframe', 'img', 'li', 'ol', 'p', 'span', 'strong', 'ul',
    'table', 'thead', 'tbody', 'tr', 'th', 'td', 'blockquote', 'code',
    'pre', 'hr', 'figure', 'figcaption'
  ]
});

// Function to invalidate cache for a specific post
const invalidateCache = (postId = null) => {
  if (postId) {
    cache.posts.delete(postId);
    cache.posts.delete('all');
  } else {
    cache.posts.clear();
  }
  cache.categories.clear();
  cache.featuredPosts.clear();
  cache.trendingPosts.clear();
};

// Helper function to check admin passphrase
const checkAdminPassphrase = (req) => {
  return req.headers['admin-access'] === process.env.ADMIN_PASSPHRASE;
};

// Rate limiter for public endpoints
const publicRateLimit = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100 // limit each IP to 100 requests per windowMs
});

// Get all blog posts with SSR meta tags
router.get('/posts', publicRateLimit, async (req, res) => {
  try {
    // Check cache first
    const cachedPosts = cache.posts.get('all');
    if (cachedPosts && Date.now() < cachedPosts.timestamp + cache.TTL) {
      return res.json(cachedPosts.data);
    }

    const [posts] = await pool.query(
      `SELECT * FROM blog_posts 
       ${!checkAdminPassphrase(req) ? "WHERE status = 'published'" : ''} 
       ORDER BY created_at DESC`
    );

    // Add meta tags to each post
    const postsWithMeta = posts.map(post => ({
      ...post,
      metaTags: generateMetaTags({
        ...post,
        canonical_url: `https://boomlify.com/blog/${post.slug}`
      })
    }));

    // Cache the results
    cache.posts.set('all', {
      data: postsWithMeta,
      timestamp: Date.now()
    });

    res.json(postsWithMeta);
  } catch (error) {
    console.error('Failed to fetch blog posts:', error);
    res.status(500).json({ error: 'Failed to fetch blog posts' });
  }
});

// Get a single blog post by slug with SSR meta tags
router.get('/posts/:slug', publicRateLimit, async (req, res) => {
  try {
    // Check cache first
    const cachedPost = cache.posts.get(req.params.slug);
    if (cachedPost && Date.now() < cachedPost.timestamp + cache.TTL) {
      return res.json(cachedPost.data);
    }

    const [posts] = await pool.query(
      `SELECT * FROM blog_posts 
       WHERE slug = ? ${!checkAdminPassphrase(req) ? "AND status = 'published'" : ''}`,
      [req.params.slug]
    );

    if (posts.length === 0) {
      return res.status(404).json({ error: 'Blog post not found' });
    }

    const post = {
      ...posts[0],
      metaTags: generateMetaTags({
        ...posts[0],
        canonical_url: `https://boomlify.com/blog/${posts[0].slug}`
      })
    };

    // Cache the result
    cache.posts.set(req.params.slug, {
      data: post,
      timestamp: Date.now()
    });

    res.json(post);
  } catch (error) {
    console.error('Failed to fetch blog post:', error);
    res.status(500).json({ error: 'Failed to fetch blog post' });
  }
});

// Create a new blog post
router.post('/posts', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const connection = await pool.getConnection();
  try {
    const {
      title,
      content,
      category,
      meta_title,
      meta_description,
      keywords,
      featured_image,
      status = 'draft',
      author,
      is_featured = false,
      is_trending = false,
      featured_order = null,
      trending_order = null,
      og_title,
      og_description,
      og_image,
      twitter_title,
      twitter_description,
      twitter_image
    } = req.body;

    if (!title || !content || !category) {
      return res.status(400).json({ error: 'Title, content and category are required' });
    }

    const slug = title
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/(^-|-$)/g, '');

    const [existingSlugs] = await connection.query(
      'SELECT id FROM blog_posts WHERE slug = ?',
      [slug]
    );

    if (existingSlugs.length > 0) {
      return res.status(400).json({ error: 'A post with this title already exists' });
    }

    const sanitizedContent = DOMPurify.sanitize(content);

    const id = uuidv4();
    await connection.query(
      `INSERT INTO blog_posts (
        id, title, slug, content, category, meta_title, 
        meta_description, keywords, featured_image, status, 
        author, created_at, updated_at, is_featured, is_trending,
        featured_order, trending_order, og_title, og_description,
        og_image, twitter_title, twitter_description, twitter_image
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW(), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        id, title, slug, sanitizedContent, category, meta_title,
        meta_description, keywords, featured_image, status,
        author, is_featured, is_trending, featured_order, trending_order,
        og_title, og_description, og_image, twitter_title,
        twitter_description, twitter_image
      ]
    );

    // Invalidate cache
    invalidateCache();

    res.json({ 
      message: 'Blog post created successfully',
      id,
      slug 
    });
  } catch (error) {
    console.error('Failed to create blog post:', error);
    res.status(500).json({ error: 'Failed to create blog post' });
  } finally {
    connection.release();
  }
});

// Update a blog post
router.put('/posts/:id', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const connection = await pool.getConnection();
  try {
    const {
      title,
      content,
      category,
      meta_title,
      meta_description,
      keywords,
      featured_image,
      status,
      author,
      og_title,
      og_description,
      og_image,
      twitter_title,
      twitter_description,
      twitter_image
    } = req.body;

    if (!title || !content || !category) {
      return res.status(400).json({ error: 'Title, content and category are required' });
    }

    const [existingPost] = await connection.query(
      'SELECT id FROM blog_posts WHERE id = ?',
      [req.params.id]
    );

    if (existingPost.length === 0) {
      return res.status(404).json({ error: 'Blog post not found' });
    }

    const slug = title
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/(^-|-$)/g, '');

    const [existingSlugs] = await connection.query(
      'SELECT id FROM blog_posts WHERE slug = ? AND id != ?',
      [slug, req.params.id]
    );

    if (existingSlugs.length > 0) {
      return res.status(400).json({ error: 'A post with this title already exists' });
    }

    const sanitizedContent = DOMPurify.sanitize(content);

    await connection.query(
      `UPDATE blog_posts SET 
        title = ?, 
        slug = ?, 
        content = ?, 
        category = ?,
        meta_title = ?, 
        meta_description = ?, 
        keywords = ?,
        featured_image = ?, 
        status = ?, 
        author = ?,
        og_title = ?,
        og_description = ?,
        og_image = ?,
        twitter_title = ?,
        twitter_description = ?,
        twitter_image = ?,
        updated_at = NOW()
       WHERE id = ?`,
      [
        title, slug, sanitizedContent, category,
        meta_title, meta_description, keywords,
        featured_image, status, author,
        og_title, og_description, og_image,
        twitter_title, twitter_description, twitter_image,
        req.params.id
      ]
    );

    // Invalidate cache
    invalidateCache(req.params.id);

    res.json({ 
      message: 'Blog post updated successfully',
      slug 
    });
  } catch (error) {
    console.error('Failed to update blog post:', error);
    res.status(500).json({ error: 'Failed to update blog post' });
  } finally {
    connection.release();
  }
});

// Delete a blog post
router.delete('/posts/:id', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const connection = await pool.getConnection();
  try {
    const [posts] = await connection.query(
      'SELECT id FROM blog_posts WHERE id = ?',
      [req.params.id]
    );

    if (posts.length === 0) {
      return res.status(404).json({ error: 'Blog post not found' });
    }

    await connection.query(
      'DELETE FROM blog_posts WHERE id = ?',
      [req.params.id]
    );

    // Invalidate cache
    invalidateCache(req.params.id);

    res.json({ message: 'Blog post deleted successfully' });
  } catch (error) {
    console.error('Failed to delete blog post:', error);
    res.status(500).json({ error: 'Failed to delete blog post' });
  } finally {
    connection.release();
  }
});

// Get blog categories
router.get('/categories', publicRateLimit, async (req, res) => {
  try {
    // Check cache first
    const cachedCategories = cache.categories.get('all');
    if (cachedCategories && Date.now() < cachedCategories.timestamp + cache.TTL) {
      return res.json(cachedCategories.data);
    }

    const [categories] = await pool.query(
      `SELECT DISTINCT category FROM blog_posts 
       ${!checkAdminPassphrase(req) ? "WHERE status = 'published'" : ''} 
       ORDER BY category`
    );

    const categoryList = categories.map(c => c.category);

    // Cache the results
    cache.categories.set('all', {
      data: categoryList,
      timestamp: Date.now()
    });

    res.json(categoryList);
  } catch (error) {
    console.error('Failed to fetch categories:', error);
    res.status(500).json({ error: 'Failed to fetch categories' });
  }
});

// Update featured/trending status
router.patch('/posts/:id/status', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const connection = await pool.getConnection();
  try {
    const { is_featured, is_trending, featured_order, trending_order } = req.body;

    await connection.query(
      `UPDATE blog_posts 
       SET is_featured = ?, is_trending = ?, featured_order = ?, trending_order = ?
       WHERE id = ?`,
      [is_featured, is_trending, featured_order, trending_order, req.params.id]
    );

    // Invalidate cache
    invalidateCache();

    res.json({ message: 'Post status updated successfully' });
  } catch (error) {
    console.error('Failed to update post status:', error);
    res.status(500).json({ error: 'Failed to update post status' });
  } finally {
    connection.release();
  }
});

// Reorder featured/trending posts
router.post('/posts/reorder', async (req, res) => {
  if (!checkAdminPassphrase(req)) {
    return res.status(403).json({ error: 'Unauthorized' });
  }

  const connection = await pool.getConnection();
  try {
    const { posts, type } = req.body; // type can be 'featured' or 'trending'
    const orderField = type === 'trending' ? 'trending_order' : 'featured_order';

    for (const post of posts) {
      await connection.query(
        `UPDATE blog_posts SET ${orderField} = ? WHERE id = ?`,
        [post.order, post.id]
      );
    }

    // Invalidate cache
    invalidateCache();

    res.json({ message: 'Posts reordered successfully' });
  } catch (error) {
    console.error('Failed to reorder posts:', error);
    res.status(500).json({ error: 'Failed to reorder posts' });
  } finally {
    connection.release();
  }
});

// Get featured posts
router.get('/posts/featured', publicRateLimit, async (req, res) => {
  try {
    // Check cache first
    const cachedPosts = cache.featuredPosts.get('all');
    if (cachedPosts && Date.now() < cachedPosts.timestamp + cache.TTL) {
      return res.json(cachedPosts.data);
    }

    const [posts] = await pool.query(
      `SELECT * FROM blog_posts 
       WHERE is_featured = true AND status = 'published'
       ORDER BY featured_order ASC, created_at DESC`
    );

    // Add meta tags to each post
    const postsWithMeta = posts.map(post => ({
      ...post,
      metaTags: generateMetaTags({
        ...post,
        canonical_url: `https://boomlify.com/blog/${post.slug}`
      })
    }));

    // Cache the results
    cache.featuredPosts.set('all', {
      data: postsWithMeta,
      timestamp: Date.now()
    });

    res.json(postsWithMeta);
  } catch (error) {
    console.error('Failed to fetch featured posts:', error);
    res.status(500).json({ error: 'Failed to fetch featured posts' });
  }
});

// Get trending posts
router.get('/posts/trending', publicRateLimit, async (req, res) => {
  try {
    // Check cache first
    const cachedPosts = cache.trendingPosts.get('all');
    if (cachedPosts && Date.now() < cachedPosts.timestamp + cache.TTL) {
      return res.json(cachedPosts.data);
    }

    const [posts] = await pool.query(
      `SELECT * FROM blog_posts 
       WHERE is_trending = true AND status = 'published'
       ORDER BY trending_order ASC, created_at DESC`
    );

    // Add meta tags to each post
    const postsWithMeta = posts.map(post => ({
      ...post,
      metaTags: generateMetaTags({
        ...post,
        canonical_url: `https://boomlify.com/blog/${post.slug}`
      })
    }));

    // Cache the results
    cache.trendingPosts.set('all', {
      data: postsWithMeta,
      timestamp: Date.now()
    });

    res.json(postsWithMeta);
  } catch (error) {
    console.error('Failed to fetch trending posts:', error);
    res.status(500).json({ error: 'Failed to fetch trending posts' });
  }
});

export default router;
