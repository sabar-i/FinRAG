import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import re
from urllib.parse import urljoin
import os

def get_page_no(url, company, page_no, next, year):
    """
    Get the maximum page number and next value for pagination
    """
    try:
        request = requests.get(url)
        request.raise_for_status()
        
        soup = BeautifulSoup(request.text, 'html.parser')
        all_page_no = soup.find_all('div', attrs={'class': 'pages MR10 MT15'})
        
        # Check if pagination elements exist
        if not all_page_no:
            print(f"No pagination found for {company} in year {year}")
            return 1, next
        
        # Check if the first element exists and has anchor tags
        if not all_page_no[0] or not all_page_no[0].find_all('a'):
            print(f"No pagination links found for {company} in year {year}")
            return 1, next
        
        page_list = [i.text for i in all_page_no[0].find_all('a')]
        
        if not page_list:
            print(f"Empty page list for {company} in year {year}")
            return 1, next
        
        # Check if the last element is a number
        if any(map(str.isdigit, page_list[-1])):
            return int(page_list[-1]), next
        else:
            # If last element is not a number, try to find the highest number
            numeric_pages = [int(p) for p in page_list if p.isdigit()]
            if numeric_pages:
                return max(numeric_pages), next
            else:
                print(f"No numeric pages found for {company} in year {year}")
                return 1, next
                
    except requests.RequestException as e:
        print(f"Request failed for {company} in year {year}: {e}")
        return 1, next
    except Exception as e:
        print(f"Error processing {company} in year {year}: {e}")
        return 1, next

def extract_full_article(article_url):
    """
    Extract full article content from a given URL
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(article_url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Try different selectors for article content
        article_selectors = [
            'div.article_scheme',
            'div.article',
            'div.content_text',
            'div.article-content',
            'div.story-content',
            'div.article-body',
            'div.content',
            'article',
            'div[class*="article"]',
            'div[class*="content"]'
        ]
        
        article_content = ""
        
        for selector in article_selectors:
            content_div = soup.select_one(selector)
            if content_div:
                # Remove script and style elements
                for script in content_div(["script", "style", "nav", "header", "footer", "aside"]):
                    script.decompose()
                
                # Extract text content
                paragraphs = content_div.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
                if paragraphs:
                    article_content = '\n\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
                    break
        
        # If no content found with selectors, try to extract from body
        if not article_content:
            body = soup.find('body')
            if body:
                # Remove unwanted elements
                for unwanted in body(["script", "style", "nav", "header", "footer", "aside", "form", "button"]):
                    unwanted.decompose()
                
                # Find all text content
                text_elements = body.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div'])
                article_content = '\n\n'.join([elem.get_text(strip=True) for elem in text_elements if elem.get_text(strip=True) and len(elem.get_text(strip=True)) > 50])
        
        # Clean up the content
        if article_content:
            # Remove extra whitespace and normalize
            article_content = re.sub(r'\s+', ' ', article_content)
            article_content = re.sub(r'\n\s*\n', '\n\n', article_content)
            article_content = article_content.strip()
            
            # Limit content length to avoid extremely long articles
            if len(article_content) > 15000:
                article_content = article_content[:15000] + "... [Content truncated]"
        
        return article_content
        
    except requests.RequestException as e:
        print(f"Failed to fetch article {article_url}: {e}")
        return "Error: Could not fetch article content"
    except Exception as e:
        print(f"Error processing article {article_url}: {e}")
        return "Error: Could not parse article content"

def save_company_data_with_full_articles(sc_id, years, max_articles_per_company=None, delay_between_articles=2):
    """
    Save company data with full article content for given stock codes and years
    
    Parameters:
    - sc_id: List of company stock codes
    - years: List of years to scrape
    - max_articles_per_company: Maximum articles per company (None for unlimited)
    - delay_between_articles: Delay in seconds between article requests
    """
    url_ = "https://www.moneycontrol.com/stocks/company_info/stock_news.php?"
    
    all_data = []
    total_articles_processed = 0
    
    print(f"Starting full article scraping for companies: {sc_id}")
    print(f"Years: {years}")
    print(f"Max articles per company: {max_articles_per_company if max_articles_per_company else 'Unlimited'}")
    print(f"Delay between articles: {delay_between_articles} seconds")
    print("=" * 60)
    
    for company in sc_id:
        for year in years:
            print(f"\nProcessing {company} for year {year}")
            
            page_no = 1
            next = 0
            
            url = url_ + 'sc_id=' + company + '&scat=&page_no=' + str(page_no) + '&next=' + str(next) + '&durationType=Y&Year=' + str(year) + '&duration=1&news_type='
            print(f'URL: {url}')
            
            max_page_no, max_next = get_page_no(url, company, page_no, next, year)
            max_next = max_next + 1
            
            articles_processed = 0
            
            for i in range(max_next):
                for j in range(1, max_page_no + 1):
                    try:
                        url = url_ + 'sc_id=' + company + '&scat=&page_no=' + str(j) + '&next=' + str(i) + '&durationType=Y&Year=' + str(year) + '&duration=1&news_type='
                        
                        request = requests.get(url)
                        request.raise_for_status()
                        
                        soup = BeautifulSoup(request.text, 'html.parser')
                        articles = soup.find_all('div', attrs={'class': 'FL PR20'})
                        
                        for article in articles:
                            # Check if we've reached the limit
                            if max_articles_per_company and articles_processed >= max_articles_per_company:
                                print(f"Reached limit of {max_articles_per_company} articles for {company}")
                                break
                                
                            try:
                                link_element = article.find('a', attrs={'class': 'arial11_summ'})
                                if not link_element:
                                    continue
                                
                                title = link_element.get('title', '') or link_element.get_text(strip=True)
                                link = link_element.get('href', 'No link')
                                
                                # Skip if no valid link
                                if link == 'No link' or not link.startswith('http'):
                                    continue
                                
                                articles_processed += 1
                                total_articles_processed += 1
                                
                                print(f"Extracting article {articles_processed} for {company} ({total_articles_processed} total): {title[:60]}...")
                                
                                # Extract full article content
                                full_article = extract_full_article(link)
                                
                                # Extract date from the article block
                                date_element = article.find('span', attrs={'class': 'g_date'})
                                if date_element:
                                    date = date_element.get_text(strip=True)
                                else:
                                    date = "No date"
                                
                                # Try to extract summary from title attribute or link text
                                summary = link_element.get('title', '') or link_element.get_text(strip=True)
                                
                                data = {
                                    'company': company,
                                    'year': year,
                                    'title': title,
                                    'link': link,
                                    'date': date,
                                    'summary': summary,
                                    'full_article': full_article
                                }
                                all_data.append(data)
                                
                                # Add delay to be respectful to the server
                                time.sleep(delay_between_articles)
                                
                            except Exception as e:
                                print(f"Error processing article for {company} in year {year}: {e}")
                                continue
                        
                        # Break outer loop if we've reached the limit
                        if max_articles_per_company and articles_processed >= max_articles_per_company:
                            break
                            
                        # Add delay between pages
                        time.sleep(1)
                        
                    except requests.RequestException as e:
                        print(f"Request failed for {company} year {year} page {j}: {e}")
                        continue
                    except Exception as e:
                        print(f"Error processing {company} year {year} page {j}: {e}")
                        continue
                
                # Break outer loop if we've reached the limit
                if max_articles_per_company and articles_processed >= max_articles_per_company:
                    break
    
    # Save to DataFrame and CSV
    if all_data:
        df = pd.DataFrame(all_data)
        
        # Create filename with timestamp
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"company_news_full_articles_{'_'.join(sc_id)}_{'_'.join(map(str, years))}_{timestamp}.csv"
        
        # Save with UTF-8 encoding
        df.to_csv(filename, index=False, encoding='utf-8')
        
        print(f"\n" + "=" * 60)
        print(f"SCRAPING COMPLETED!")
        print(f"Data saved to: {filename}")
        print(f"Total articles collected: {len(all_data)}")
        print(f"File size: {os.path.getsize(filename) / (1024*1024):.2f} MB")
        
        # Show sample statistics
        if not df.empty:
            print(f"\nSample statistics:")
            print(f"- Average article length: {df['full_article'].str.len().mean():.0f} characters")
            print(f"- Shortest article: {df['full_article'].str.len().min()} characters")
            print(f"- Longest article: {df['full_article'].str.len().max()} characters")
            
            print(f"\nSample article preview:")
            sample_article = df.iloc[0]['full_article']
            print(f"Length: {len(sample_article)} characters")
            print(f"Preview: {sample_article[:300]}...")
        
        return df
    else:
        print("No data collected")
        return None

# Example usage
if __name__ == "__main__":
    # Example 1: Test with limited articles
    # print("TESTING WITH LIMITED ARTICLES")
    # result = save_company_data_with_full_articles(
    #     sc_id=['RI'], 
    #     years=[2025], 
    #     max_articles_per_company=None,
    #     delay_between_articles=1
    # )
    
    # Example 2: Full scraping (uncomment to use)
    print("\nFULL SCRAPING")
    result = save_company_data_with_full_articles(
        sc_id = [
    'HDF01',    # HDFC Bank
    'ICI02',   # ICICI Bank
    'SBI',    # State Bank of India
    'KMB',   # Kotak Mahindra Bank
    'AB16',   # Axis Bank
    'BOB',    # Bank of Baroda    
    'PNB05 ',      # Punjab National Bank
    'UBI01',    # Union Bank of India
    'CB06',   # Canara Bank
    'IIB',   # IndusInd Bank
    'BAF',    # Bajaj Finance
    'MF10',   # Muthoot Finance
    'STF',   # Shriram Finance
    'CDB', # Cholamandalam Invest.
    'LFH',   # L&T Finance Holdings
    'ABC9',   # Aditya Birla Finance
    'MMF04',   # Mahindra Finance
    'MF20',   # Poonawalla Fincorp
    'RI',   # Reliance Industries
    'TCS',    # Tata Consultancy Services (TCS)
    'IT',   # Infosys
    'BA08', #Airtel
    'HU',
    'LIC09']
, 
        years=[2025], 
        max_articles_per_company=None,  # Unlimited
        delay_between_articles=2
    ) 