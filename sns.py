import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns;

sns.set()

from sklearn.datasets import load_diabetes


def fun(x):
    if x > 0:
        return 1
    else:
        return 0


# sklearn自带数据 diabetes 糖尿病数据集
diabetes = load_diabetes()
data = pd.DataFrame(diabetes.data, columns=diabetes.feature_names)
# 只抽取前80个数据
df = data[:80]
# 由于diabetes中的数据均已归一化处理过，sex列中的值也归一化，现将其划分一下，大于0的设置为1，小于等于0的设置为0
df['sex'] = df['sex'].apply(lambda x: fun(x))
"""
案例5：使用标记来标识组，而不用破折号来标识组：设置markers为True,设置dashes为False
"""
sns.lineplot(x="age", y="s1", hue="sex", style="sex", markers=True, dashes=False, data=df)
plt.show()
