# non-parametric estimation
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# df = pd.read_csv("us-west-2a_k80.csv")
df = pd.read_csv("us-west-2b_v100.csv")
df['Lifetime_minutes'] = df['Lifetime'] / 60.0

# generating cdf and survival function
def calculate_cdf_and_survival(lifetimes_minutes):
    sorted_lifetimes = np.sort(lifetimes_minutes)
    cdf = np.arange(1, len(sorted_lifetimes) + 1) / len(sorted_lifetimes)
    survival = 1 - cdf
    return sorted_lifetimes, cdf, survival

x_vals, cdf_vals, survival_vals = calculate_cdf_and_survival(df['Lifetime_minutes'].values)
plt.figure(figsize=(10, 5))

# CDF
plt.subplot(1, 2, 1)
plt.plot(x_vals, cdf_vals, drawstyle='steps-post')
plt.title("CDF of Spot VM Lifetimes")
plt.xlabel("Lifetime (minutes)")
plt.ylabel("CDF")
plt.grid(True)

# Survival Function
plt.subplot(1, 2, 2)
plt.plot(x_vals, survival_vals, drawstyle='steps-post', color='orange')
plt.title("Survival Function (1 - CDF)")
plt.xlabel("Lifetime (minutes)")
plt.ylabel("Survival Probability")
plt.grid(True)

plt.tight_layout()
plt.savefig("spot_vm_lifetime_analysis.png")

# calculate cond prob.
def conditional_survival_probability(t, delta_t, x_vals, survival_vals):
    s_t = np.interp(t, x_vals, survival_vals)
    s_t_dt = np.interp(t + delta_t, x_vals, survival_vals)
    return s_t_dt / s_t if s_t > 0 else 0.0

# test code...
initial_delta = 15  # next 30min + current time
print("Conditional Survival Probability Test (for next 300 minutes):\n")
for t in range(0, 900, 5):  # 5min interval from start to 900m
    prob = conditional_survival_probability(t, initial_delta, x_vals, survival_vals)
    #print(prob)
    print(f"At t = {t} min --> P(alive for next {initial_delta} min, which is at {t + initial_delta}) = {prob:.2%}")