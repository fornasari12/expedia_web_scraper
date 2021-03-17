from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import logging
from time import sleep
import pandas as pd
import datetime as dt
from datetime import datetime
from google.cloud import bigquery
from utils import BigQueryClient
import pytz

logging.getLogger().setLevel(logging.INFO)
# path to selenium web driver:
web_driver = "/Users/nicolasfornasari/OneDrive/19_ALPHALABS/webdriver/chromedriver_mac"


class ExpediaScraper:

    def __init__(self, start_date, end_date):
        self.date_range = pd.date_range(start=start_date, end=end_date)
        self.hotels = ['Punta-Del-Este-Hoteles-Hotel-Fasano-Punta-Del-Este.h4100132',
                       'Punta-Del-Este-Hoteles-The-Grand-Hotel-Punta-Del-Este.h7017144']
        self.chrome_options = Options()
        self.chrome_options.add_argument("--window-position=500,0")
        self.driver = webdriver.Chrome(web_driver, chrome_options=self.chrome_options)
        self.tz = pytz.timezone('America/Montevideo')

    def get_room_data(self, hotel, checkin, checkout):

        url = f'https://www.expedia.com/es/' \
              f'{hotel}' \
              f'.Informacion-Hotel' \
              f'?chkin={checkin}' \
              f'&chkout={checkout}' \
              '&destType=MARKET' \
              '&destination=Punta%20del%20Este%2C%20Maldonado%2C%20Uruguay' \
              '&pwa_ts=1606948486146' \
              '&referrerUrl=aHR0cHM6Ly93d3cuZXhwZWRpYS5jb20vSG90ZWwtU2VhcmNo' \
              '&regionId=2756' \
              '&rfrr=HSR' \
              '&rm1=a2' \
              '&selectedRatePlan=228238012' \
              '&selectedRoomType=211212798' \
              '&semdtl=' \
              '&sort=PROPERTY_CLASS' \
              '&top_cur=USD' \
              '&top_dp=380' \
              '&useRewards=false' \
              '&x_pwa=1'
        self.driver.get(url)
        sleep(3)
        x_path_str = "//li[contains(@data-stid,'section-roomtype')]"
        room_box = self.driver.find_elements_by_xpath(x_path_str)
        rooms_data = []

        for room in room_box:
            room_name = room.find_element_by_tag_name('h3').text
            ammenities = room.find_elements_by_class_name('all-l-padding-two')
            ammenities = [a.text for a in ammenities]

            if len(ammenities) > 0:
                try:
                    room_size = ammenities[0]
                    room_capacity = ammenities[1]
                    bed_types = ammenities[2]
                except:
                    room_size = 'NA'
                    room_capacity = 'NA'
                    bed_types = 'NA'
            else:
                room_size = 'NA'
                room_capacity = 'NA'
                bed_types = 'NA'

            x_path_str = ".//span[contains(@data-stid,'hotel-lead-price')]"
            prices_elements = room.find_elements_by_xpath(x_path_str)
            prices = [p.text for p in prices_elements]

            if len(prices) == 1:
                price_option_1 = prices[0].replace('$', '')
                price_option_2 = 0
            else:
                price_option_1 = prices[0].replace('$', '')
                price_option_2 = prices[1].replace('$', '')

            room_data = {
                         'hotel': hotel.replace('Punta-Del-Este-Hoteles-', '')
                                       .replace('-', ' ')
                                       .replace('.h4100132', '')
                                       .replace('.h7017144', ''),
                         'execution_date': datetime.now(tz=self.tz).date(),
                         'execution_timestamp': datetime.utcnow(),
                         'checkin_date': checkin,
                         'checkout_date': checkout,
                         'room_name': room_name.replace('Superior Room',
                                                        'Habitación superior')
                                               .replace('Deluxe Room',
                                                        'Habitación Deluxe')
                                               .replace('2 Twin Beds',
                                                        '2 camas individuales'),
                         'room_size': room_size,
                         'room_capactity': room_capacity,
                         'bed_types': bed_types,
                         'price_option_1': float(price_option_1),
                         'price_option_2': float(price_option_2)
            }

            rooms_data.append(room_data)
        return rooms_data

    def iterate_through_date_range(self, days_delta=2):
        df_rooms = pd.DataFrame()
        for hotel in self.hotels:
            for date in self.date_range:
                checkin = date.date()
                checkout = (date + dt.timedelta(days=days_delta)).date()
                df_rooms = df_rooms.append(pd.DataFrame(self.get_room_data(hotel, checkin, checkout)))

        self.driver.quit()
        print('driver quited')
        return df_rooms

logging.info('Starting scraping')
bot = ExpediaScraper(start_date='2021-03-01', end_date='2021-03-31')

logging.info('Starting with webdriver scraping')
df = bot.iterate_through_date_range().reset_index(drop=True)
logging.info('Finished with webscraping')

schema = [
    bigquery.SchemaField("hotel", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("execution_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("execution_timestamp", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("checkin_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("checkout_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("room_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("price_option_1", "FLOAT64", mode="NULLABLE"),
          ]
table_id = 'revenue-manager-alphalabs.revenue_manager.hotels_table'

df = df[['hotel', 'execution_date', 'execution_timestamp', 'checkin_date',
         'checkout_date', 'room_name', 'price_option_1']]


logging.info('Starting BigQuery operations')
BigQueryClient(schema=schema,
               table_id=table_id,
               df=df).create_table()
logging.info('BigQuery Table created if needed')
BigQueryClient(schema=schema,
               table_id=table_id,
               df=df).upload_dataframe_to_table(truncate=False)
logging.info('Finished to upload data to BigQuery')