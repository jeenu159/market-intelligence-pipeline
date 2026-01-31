import os
import csv
import time
import requests
from groq import Groq
from dotenv import load_dotenv

# Load API keys from .env
load_dotenv()

# 1. Setup Groq Client
# Ensure GROQ_API_KEY is in your .env file
client = Groq(
    api_key=os.getenv("GROQ_API_KEY"),
)

def get_articles():
    """Fetches raw articles from NewsAPI"""
    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        print("Error: NEWS_API_KEY not found in .env")
        return []

    # We search for diverse topics to ensure our dataset has variety
    url = f"https://newsapi.org/v2/everything?q=(technology OR business OR health OR science)&pageSize=50&apiKey={api_key}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json().get('articles', [])
    except Exception as e:
        print(f"Error fetching news: {e}")
        return []

def clean_text(text):
    """Removes newlines and extra spaces so CSV doesn't break"""
    if not text: return ""
    return text.replace('\n', ' ').replace('\r', '').strip()

def get_ai_label(title, description):
    """Asks Groq (Llama 3) to categorize the article"""
    content = f"Title: {title}\nDescription: {description}"
    
    prompt = f"""
    You are a news classifier. Categorize the following news article into exactly ONE of these categories:
    - Technology
    - Business
    - Health
    - Science
    - Other

    Article:
    {content}

    Reply ONLY with the category name. Do not add punctuation or explanation.
    """
    
    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="llama-3.3-70b-versatile",# Using Llama 3 (Fast & Free on Groq)
        )
        return chat_completion.choices[0].message.content.strip()
    except Exception as e:
        print(f"AI Error: {e}")
        return "Error"

def main():
    print("1. Fetching articles from NewsAPI...")
    articles = get_articles()
    print(f"   Found {len(articles)} articles.\n")
    
    dataset = []
    
    print("2. Asking Groq (Llama 3) to label them...")
    print("-" * 50)
    
    for i, article in enumerate(articles):
        title = clean_text(article.get('title'))
        description = clean_text(article.get('description'))
        
        # Skip if data is missing
        if not title or not description:
            continue
            
        # Get label from AI
        label = get_ai_label(title, description)
        
        # Print progress so you know it's working
        print(f"[{i+1}/{len(articles)}] {label}: {title[:40]}...")
        
        # Save to list
        dataset.append([title + ". " + description, label])
        
        # Groq is fast, but let's add a tiny sleep just to be safe
        time.sleep(0.5)

    # 3. Save to CSV
    print("-" * 50)
    print("3. Saving to 'training_data.csv'...")
    with open('training_data.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['text', 'category']) # The Header
        writer.writerows(dataset)
    
    print("Done! You now have a dataset.")

if __name__ == "__main__":
    main()