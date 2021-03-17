from utils import BigQueryClient
import matplotlib.pyplot as plt

query = """SELECT * FROM `revenue-manager-alphalabs.revenue_manager.hotels_table`"""

df = BigQueryClient().load_dataframe_from_bigquery(query=query)

room = df[df['room_name'] == 'Habitación estándar, vistas al mar'].reset_index(drop=True)

room_pivot = room[['execution_date', 'checkin_date', 'price']].pivot_table(index='execution_date', columns='checkin_date', values='price')

price = room[['checkin_date', 'price']].set_index('checkin_date')

room.plot()
plt.show(block=False)
plt.pause(10)
plt.close('all')

print('ca')