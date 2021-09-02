import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from numpy.polynomial.polynomial import polyfit

plt.style.use('ggplot')

df = pd.read_csv('results.csv')
x = df["size"]
y = df["average_time"]

intercept, gradiant = polyfit(x, y, 1)
get_predicted_duration_seconds = lambda filesize: gradiant * filesize + intercept

bestfit_x = [df["size"].min(), df["size"].max()]
bestfit_y = list(map(get_predicted_duration_seconds, bestfit_x))

sns.scatterplot(x="size",y="average_time",data=df, color="#D47674")
plt.plot(bestfit_x, bestfit_y, linestyle="dashed", color="#54A9EB")
plt.title("Performance curve of airflow pipeline")
plt.ylabel("Average Duration (Seconds)")
plt.xlabel("Number of Sentences")
plt.savefig('performance_curve.png')
plt.show()
