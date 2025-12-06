import random
import pandas as pd
import logging
logging.basicConfig(format='%(levelname)s-%(message)s')
from datetime import datetime
import os

menu = [{'hot_beverages': {'Americano': 185, 
                           'Espresso': 135,
                            'Flat White': 350,
                            'Cafe Latte': 330,
                            'Cappucino': 310,
                            'Doppio': 200,
                            'Macchiato': 240,
                            'Caramel Macchiato': 310,
                            'Espresso Con Panna': 215,
                            'Espresso Affogato': 250,
                            'Americano Royale': 375,
                            'Piccolo': 260,
                            'Australian Style Cappucino': 350},

        'iced_coffees': {'Iced Latte': 495,
                         'Iced Cappucino': 495,
                         'Iced Americano': 395,
                         'Iced Caramel Macchiato': 580,
                         'Blended/Iced Mocha': 550,
                         'Blended Double Vanilla Mocha': 590,
                         'Blended Caramel Machiato': 610},

        'alternative drinks': {'Nepali Tea': 210,
                               'Hot Lemon With Honey': 260,
                               'Hot Chocolate': 340,
                               'Flavored Exotic Tea': 300,
                               'Iced Peach Tea Or Lemon Tea': 450,
                               'Fresh Seasonal Juice': 415,
                               'Seasonal Smoothies': 465,
                               "Oreo Cookie Shake": 585,
                               "Ice Cream Shake": 475,
                               'Blended Mint Lemonade': 385},

        'sandwich': {'American Club': 785,
                     'Italian Club': 985,
                     'Veg Club': 645,
                     'Classic BLT': 745,
                     'Steak Sandwich': 955,
                     'Salami Sandwich': 955,
                     'Ham & Cheese Sandwich': 855,
                     },

        'salads': {'Grilled Chicken Salad': 845,
                   'Organic Salad': 650,
                   'Quinoa Salad': 775,
                   'Cesar Salad Veg': 550,
                   'Cesar Salad Chicken': 795},

        'bagels': {'Bagels with Egg and Cheese': 665,
                   'Bagels with Bacon, Egg and Cheese': 745,
                   'Bagels with Ham and Cheese': 465,
                   'Bagels with Cream Cheese': 425},
                            
        'eggs': {'Two Eggs': 510,
                      'Two Eggs with Bacon or Sausage': 610,
                      'Two Eggs with Spinach and Cheese': 525,
                      'Three Eggs': 555,
                      'Three Eggs with Bacon or Sausage': 710,
                      },
                      
        'breakfast': {'Original Triple Layer Pancake': 895,
                      'French Toast': 775,
                      'Big Breakfast': 985,
                      'Museli': 755,
                      'Crossiant with Bacon Egg & Cheese': 920,
                      'Breakfast Creals': 665,
                      'Waffle': 775,
                      'Poach Benedict': 775}
                      }]

menu_list = []
item_id_counter = 1
now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for category, items in menu[0].items():
    for item, price in items.items():
        menu_list.append({
            'item_id': f"{category[:3].upper()}-{item_id_counter}",
            "item_name": item,
            "category": category,
            "price": price,
            "created_date": now
        })
        item_id_counter+=1

df_menu = pd.DataFrame(menu_list)
print(df_menu.head(5))

path = 'C:/Users/acer/Desktop/Datasets/coffee_house'
filepath = os.path.join(path, 'menu.csv')

# ensuring the path exists
os.makedirs(path, exist_ok=True)

df_menu.to_csv(filepath, index=False)
logging.info(f"{df_menu} saved to {filepath}")
