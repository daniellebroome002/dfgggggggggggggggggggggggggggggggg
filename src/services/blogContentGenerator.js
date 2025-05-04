import axios from 'axios';
import { v4 as uuidv4 } from 'uuid';

export class BlogContentGenerator {
  constructor() {
    this.apiKey = process.env.DEEPSEEK_API_KEY;
    this.apiEndpoint = 'https://api.deepseek.com/v1/chat/completions';
  }

  async callDeepSeekAPI(prompt) {
    try {
      const response = await axios.post(
        this.apiEndpoint,
        {
          model: 'deepseek-chat',
          messages: [{ role: 'user', content: prompt }],
          temperature: 0.7,
          max_tokens: 2000
        },
        {
          headers: {
            'Authorization': `Bearer ${this.apiKey}`,
            'Content-Type': 'application/json'
          }
        }
      );

      return response.data.choices[0].message.content;
    } catch (error) {
      console.error('DeepSeek API error:', error);
      throw new Error('Failed to generate content');
    }
  }

  generatePrompt(config) {
    return `
      Create a high-quality blog post about ${config.topic}
      Type: ${config.type}
      Word count: ${config.wordCount}
      Tone: ${config.tone}
      Keywords to include: ${config.keywords.join(', ')}
      
      The content should:
      1. Be well-structured with proper headings (H2, H3)
      2. Include an engaging introduction
      3. Provide actionable insights and valuable information
      4. Have a clear conclusion
      5. Be SEO-optimized but natural sounding
      6. Include relevant examples and explanations
      
      Format the response as JSON with the following structure:
      {
        "title": "SEO-optimized title",
        "content": "Full HTML content with proper heading tags",
        "meta_description": "160 character meta description",
        "keywords": "comma-separated keywords",
        "excerpt": "Brief excerpt for preview",
        "category": "appropriate category",
        "slug": "url-friendly-slug"
      }
    `;
  }

  async generatePost(config) {
    const prompt = this.generatePrompt(config);
    const response = await this.callDeepSeekAPI(prompt);
    
    try {
      const content = JSON.parse(response);
      return {
        ...content,
        id: uuidv4(),
        created_at: new Date().toISOString(),
        status: 'draft',
        featured_image: '',
        engagement_settings: {
          allow_comments: true,
          show_share_buttons: true
        }
      };
    } catch (error) {
      console.error('Failed to parse generated content:', error);
      throw new Error('Invalid content format');
    }
  }

  async generateTopics(category, count = 5) {
    const prompt = `
      Generate ${count} engaging blog post topics about ${category}.
      Topics should be:
      1. Relevant to current trends
      2. SEO-optimized
      3. Interesting to readers
      4. Unique and not generic
      
      Format the response as a JSON array of strings.
    `;

    const response = await this.callDeepSeekAPI(prompt);
    return JSON.parse(response);
  }

  async validateContent(content) {
    const prompt = `
      Analyze this blog post content for quality:
      ${content}
      
      Check for:
      1. Plagiarism indicators
      2. Grammar and spelling
      3. Content accuracy
      4. SEO optimization
      5. Readability
      
      Return a JSON object with pass/fail and reasons.
    `;

    const response = await this.callDeepSeekAPI(prompt);
    const validation = JSON.parse(response);
    return validation.pass;
  }
}
