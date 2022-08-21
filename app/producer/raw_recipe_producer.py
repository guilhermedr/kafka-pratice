import requests
from time import sleep
from bs4 import BeautifulSoup



def fetch_raw(recipe_url):
    html = None
    print('Processing..{}'.format(recipe_url))
    try:
        r = requests.get(recipe_url,headers = headers)
        if r.status_code == 200:
            html = r.text
    
    except Exception as ex:
        print('Exception while accessing raw html')
        print(str(ex))
    finally:
        return html.strip()

def get_recipe():
    recipes = []
    salad_url = 'https://www.allrecipes.com/recipes/96/salad/'
    url = 'https://www.allrecipes.com/recipes/96/salad/'
    print('Acessing list')

    try:
        r = requests.get(url,headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BealtifulSoap('html','lxml')
            links = soup.select('.fixed-recipe-card__h3 a')
            idx = 0
            for link in links:
                sleep(2)
                recipe = fetch_raw(link['href'])
                recipes.append(recipe)
                idx += 1
                if idx > 2:
                    break
    except Exception as ex:
        print('Exception in get_recipes')
        print(str(ex))
    finally:
        return recipes

