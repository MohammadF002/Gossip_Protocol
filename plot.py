import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results.csv", names=["conv", "over"])

mean_conv = df["conv"].mean()
std_conv = df["conv"].std()

mean_over = df["over"].mean()
std_over = df["over"].std()

plt.figure()
plt.title("Convergence Time")
plt.bar(["Mean"], [mean_conv], yerr=[std_conv])
plt.show()

plt.figure()
plt.title("Message Overhead")
plt.bar(["Mean"], [mean_over], yerr=[std_over])
plt.show()