import requests
from bs4 import BeautifulSoup
import json
import time

ELASTICSEARCH_ENDPOINT = "https://my-elasticsearch-project-c0f5a6.es.us-central1.gcp.elastic.cloud:443"
API_KEY = "YOUR_API_KEY"
INDEX_NAME = "tech-learning-index"

def fetch_search_labs_blogs():
    """Scrape Elastic Search Labs blog listing with pagination"""
    articles = []
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
    blog_urls = set()
    
    # Fetch multiple pages (pagination)
    for page in range(1, 4):  # Pages 1, 2, 3
        url = f"https://www.elastic.co/search-labs/blog?page={page}"
        print(f"Fetching page {page}...")
        
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Find blog links
            links = soup.find_all('a', href=True)
            for link in links:
                href = link.get('href', '')
                if '/search-labs/blog/' in href and href != '/search-labs/blog/' and href != '/search-labs/blog':
                    full_url = f"https://www.elastic.co{href}" if href.startswith('/') else href
                    full_url = full_url.split('?')[0].rstrip('/')
                    blog_urls.add(full_url)
            
            time.sleep(1)  # Be polite
            
        except Exception as e:
            print(f"  Error fetching page {page}: {e}")
    
    print(f"\nFound {len(blog_urls)} unique blog URLs")
    
    # Fetch each blog article (limit to 30)
    blog_urls = list(blog_urls)[:30]
    for i, blog_url in enumerate(blog_urls):
        try:
            print(f"  [{i+1}/{len(blog_urls)}] {blog_url.split('/')[-1][:45]}...")
            resp = requests.get(blog_url, headers=headers, timeout=30)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Extract title
            title_tag = soup.find('h1')
            title = title_tag.get_text(strip=True) if title_tag else None
            
            # Extract content
            paragraphs = soup.find_all('p')
            content = ' '.join([p.get_text(strip=True) for p in paragraphs[:15]])
            
            if title and content and len(content) > 100:
                articles.append({
                    "title": title,
                    "content": content[:3000],
                    "url": blog_url,
                    "source": "elastic-search-labs"
                })
                print(f"       ✓ {title[:50]}...")
            
            time.sleep(0.5)  # Be polite
                
        except Exception as e:
            print(f"       ✗ Error: {e}")
    
    return articles

def main():
    headers = {"Authorization": f"ApiKey {API_KEY}", "Content-Type": "application/json"}
    
    print("="*50)
    print("Elastic Search Labs Blog Indexer")
    print("="*50)
    
    # Delete and create index
    print("\nCreating index...")
    requests.delete(f"{ELASTICSEARCH_ENDPOINT}/{INDEX_NAME}", headers=headers)
    mapping = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "content": {"type": "semantic_text"},
                "url": {"type": "keyword"},
                "source": {"type": "keyword"}
            }
        }
    }
    resp = requests.put(f"{ELASTICSEARCH_ENDPOINT}/{INDEX_NAME}", headers=headers, json=mapping)
    print("Index created!" if resp.ok else f"Error: {resp.text}")
    
    # Fetch articles
    print("\n" + "-"*50)
    articles = fetch_search_labs_blogs()
    print("-"*50)
    
    print(f"\nTotal articles: {len(articles)}")
    
    if articles:
        print("Inserting into Elasticsearch...")
        bulk = "".join(json.dumps({"index": {}}) + "\n" + json.dumps(a) + "\n" for a in articles)
        resp = requests.post(
            f"{ELASTICSEARCH_ENDPOINT}/{INDEX_NAME}/_bulk",
            headers={**headers, "Content-Type": "application/x-ndjson"},
            data=bulk
        )
        print("✓ Done!" if resp.ok else f"Error: {resp.text}")

if __name__ == "__main__":
    main()
