import json
import requests
import os
import sys 
sys.path.append(f"{os.getenv('GIT_REPO_KAFKA_PRATICE')}/app/common_functions/")

from time import sleep
from bs4 import BeautifulSoup
from common_functions import KafkaConnectConsumer, KafkaConnectProducer, KafkaPublishMessage, getKafkaBootstrapServer

BOOTSTRAP_SERVERS = getKafkaBootstrapServer()

def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url)
        if r.status_code == 200:
            html = r.text
    
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()

def get_recipes():
    recipes = []
    url = 'https://www.allrecipes.com/recipes/96/salad/'
    print('Acessing list')

    try:
        r = requests.get(url)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html,'html.parser')
            links = soup.find_all("div",attrs={"class":"category-page-list-content category-page-list-content__recipe-card karma-main-column"})
            idx = 0
            for link in links[0].find_all("div",attrs={"class":"card__detailsContainer"}):
                sleep(2)
                recipe = fetch_raw(link.div.a['href'])
                recipes.append(recipe)
                idx += 1
                if idx > 2:
                    break
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return recipes

if __name__ == '__main__':
    
    all_recipes = get_recipes()
    raw_topic_name = 'raw_recipes'
    if len(all_recipes) > 0:
        producer = KafkaConnectProducer(BOOTSTRAP_SERVERS)
        for recipe in all_recipes:
            KafkaPublishMessage(producer, raw_topic_name, 'raw', recipe.strip())
        if producer is not None:
            producer.close()
