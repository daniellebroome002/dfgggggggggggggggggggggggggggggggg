import { BlogContentGenerator } from './blogContentGenerator.js';
import axios from 'axios';

export class ContentScheduler {
  constructor() {
    this.generator = new BlogContentGenerator();
  }

  schedule = {
    'Monday': ['how-to'],
    'Wednesday': ['review'],
    'Friday': ['comparison']
  };

  categories = [
    'Email Security',
    'Privacy Protection',
    'Digital Identity',
    'Online Safety',
    'Data Protection',
    'Cybersecurity'
  ];

  async executeSchedule() {
    const today = new Date().toLocaleDateString('en-US', { weekday: 'long' });
    const contentTypes = this.schedule[today];

    if (!contentTypes) return;

    for (const type of contentTypes) {
      try {
        // 1. Generate topics
        const category = this.categories[Math.floor(Math.random() * this.categories.length)];
        const topics = await this.generator.generateTopics(category);
        
        // 2. Generate content
        const content = await this.generator.generatePost({
          topic: topics[0],
          type: type,
          wordCount: 1500,
          tone: 'professional',
          keywords: topics[0].split(' ')
        });

        // 3. Validate content
        const isValid = await this.generator.validateContent(content.content);
        
        if (isValid) {
          // 4. Post to blog
          await axios.post(
            `${process.env.API_URL}/blog/posts`,
            content,
            {
              headers: {
                'Admin-Access': process.env.ADMIN_PASSPHRASE
              }
            }
          );

          console.log(`Successfully published: ${content.title}`);
        }
      } catch (error) {
        console.error('Failed to generate/publish content:', error);
      }
    }
  }
}
