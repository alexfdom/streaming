import os
import pandas as pd
import requests
from bs4 import BeautifulSoup


url = "https://punchng.com/topics/news/"
response = requests.get(url)

# test if the request was successful
print(f"url: {url}") # <Response [200]> means the request was successful

text = response.text

script_file = os.path.realpath(__file__)
script_path = os.path.dirname(script_file)
data_dir = os.path.join(script_path, "data")
html_path = os.path.join(data_dir, "html")
html_file_path = os.path.join(html_path, "main.html")

if not os.path.exists(html_path):
    os.makedirs(html_path)

with open(html_file_path, "w") as file:
    file.write(text)

soup = BeautifulSoup(text, "html.parser")
article_container = soup.find("div", class_="latest-news-timeline-section")

article_temp = article_container.find_all_next("article")

articles = []

for article in article_temp:
    print(article)
    
    title = article.find("h1", "post-title").text.strip()
    excerpt = article.find("p", "post-excerpt").text.strip()
    date = article.find("span", "post-date").text.strip()
    link = article.find("a")["href"]

    """   
    print("\n")
    print({
        "title": title,
        "excerpt": excerpt,
        "date": date,
        "link": link
    }) 
      """

    articles.append({
        "title": title,
        "excerpt": excerpt,
        "date": date,
        "link": link
    })
    """
    break
      """
""" print(articles) """

#punch_df = pd.DataFrame(articles)
#print(punch_df)
#print(articles[0])

for article in articles:
    article_link = article["link"]
    print(article_link)
    article_page_response = requests.get(article_link)
    article_page_text = article_page_response.text.strip()
    article_soup = BeautifulSoup(article_page_text, "html.parser")
    
    """ author= article_soup.find("span", class_="post-author").text
    content= article_soup.find("div", class_="post-content").text
    image= article_soup.find("div", class_="post-image-wrapper").find_next("figure").find_next("img")["src"]
    print(author,image) """

    article["author"] = article_soup.find("span", class_="post-author").text.strip().replace("By", "")
    article["content"] = article_soup.find("div", class_="post-content").text.strip()
    article["image"] = article_soup.find("div", class_="post-image-wrapper").find_next("figure").find_next("img")["src"]

    # break

punch_df = pd.DataFrame(articles)
print(punch_df)

script_file = os.path.realpath(__file__)
script_path = os.path.dirname(script_file)

data_dir = os.path.join(script_path, "data")
file_path = os.path.join(data_dir, "punch.csv")

if not os.path.exists(data_dir):
    os.makedirs(data_dir)

with open(file_path, "w") as file:
    punch_df.to_csv(file, index=False)

print("Done")