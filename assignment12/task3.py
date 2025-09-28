# Task 3: Interactive Visualizations with Plotly

import plotly.express as px
import plotly.data as pldata
import pandas as pd

df = pldata.wind(return_type='pandas')
print(df.head(10))

df['strength'] = df['strength'].str.replace(r"[^0-9.]", "", regex=True).astype(float)

fig = px.scatter(df, x='strength', y='frequency', color='direction', title='Wind Strength vs Frequency by Direction', labels={'strength': 'Strength (float)', 'frequency': 'Frequency'})
fig.write_html("wind.html", auto_open=True)
print(f"Interactive plot saved as 'wind.html'.")
