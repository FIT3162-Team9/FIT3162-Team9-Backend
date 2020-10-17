import pandas as pd

#Read the data csv file and a csv file contains the suburb and mapping lga
excel1 = "VICWeather.csv"
excel2 = "weatherVIC.csv"

df1 = pd.read_csv(excel1)

df2 = pd.read_csv(excel2)

#Merge two dataframes into one csv file
Left_join = pd.merge(df1,
                     df2,
                     on =['location','Year','Month','Day'],
                     #on =['Bbureau of Meteorology station number'],
                     how ='left')

#write to file
Left_join.to_csv('output.csv')



"""
ballarat = ballarat north
bendigo - greater bendigo
sale - wellington
melbourne airport - brimbank
melbourne - melbourne
mildura - mildura shire
nhil - horsham
portland - glenelg
watsonia - banyule
dartmoor - glenelg
"""