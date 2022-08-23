import json
import os
import sys 
sys.path.append(f"{os.getenv('GIT_REPO_KAFKA_PRATICE')}/app/common_functions/")

from time import sleep
from bs4 import BeautifulSoup
from common_functions import KafkaConnectConsumer, KafkaConnectProducer, KafkaPublishMessage, getKafkaBootstrapServer

BOOTSTRAP_SERVERS = getKafkaBootstrapServer()

def parse(markup):
    title = '-'
    submit_by = '-'
    description = '-'
    calories = 0
    ingredients = []
    rec = {}

    try:
        soup = BeautifulSoup(markup, 'html.parser')
        # title
        title_section = soup.find_all('h1',attrs={"class": "headline heading-content elementFont__display"})        
        # submitter
        submitter_section = soup.find_all('a',attrs={"class": "author-name author-text__block elementFont__detailsLinkOnly--underlined elementFont__details--bold"})
        # description
        description_section = soup.find_all('div',attrs={"class": "recipe-summary elementFont__dek--within"})
        # ingredients
        ingredients_section = soup.find_all('li',attrs={"class": "ingredients-item"})        
        # calories
        calories_section = soup.find_all('div',attrs={"class": "nutrition-top light-underline elementFont__subtitle"})
        
        if calories_section:
            calories_text = calories_section[0].text
            string_to_find = "Calories: "
            len_string_to_find = len(string_to_find)        
            indice_string = calories_text.find(string_to_find)            
            calories = calories_text[(indice_string+len_string_to_find):].strip()

        if ingredients_section:
            for ingredient in ingredients_section:
                ingredient_text = ingredient.label.span.span.text.strip()
                if 'Add all ingredients to list' not in ingredient_text and ingredient_text != '':
                    ingredients.append({'step': ingredient_text})

        if description_section:
            description = description_section[0].p.text.strip().replace('"', '')

        if submitter_section:
            submit_by = submitter_section[0].text.strip()

        if title_section:
            title = title_section[0].text

        rec = {'title': title, 'submitter': submit_by, 'description': description, 'calories': calories,
               'ingredients': ingredients}

    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)

if __name__ == '__main__':
    print('Runing Consumer..')
    parsed_records = []
    raw_topic_name = 'raw_recipes'
    parsed_topic_name = 'parsed_recipes'

    consumer = KafkaConnectConsumer(topic_name=raw_topic_name, \
                                    auto_offset_reset='earliest', \
                                    bootstrap_servers=BOOTSTRAP_SERVERS)

    for msg in consumer:
        html = msg.value
        result = parse(html)
        parsed_records.append(result)
    consumer.close()
    sleep(5)

    if len(parsed_records) > 0:
        print('Publishing records..')
        producer = KafkaConnectProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
        for rec in parsed_records:
            KafkaPublishMessage(producer, parsed_topic_name, 'parsed', rec)
            print(str(rec))
        if producer is not None:
            producer.close()