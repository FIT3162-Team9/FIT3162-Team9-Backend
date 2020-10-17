import pandas as pd

excel1 = "combinedmax.csv"
excel2 = "combinedmin.csv"

df1 = pd.read_csv(excel1)

df2 = pd.read_csv(excel2)

Left_join = pd.merge(df1,
                     df2,
                     on =['Bureau of Meteorology station number','Year','Month','Day'],
                     #on =['Bbureau of Meteorology station number'],
                     how ='left')
print(Left_join)
Left_join.to_csv('output.csv')