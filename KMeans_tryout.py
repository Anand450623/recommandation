import pandas as pd
from matplotlib import pyplot as plt
from sklearn.cluster import KMeans
from sklearn import preprocessing


def main():

    user_data = pd.read_csv('user_analysis.csv')
    unwanted_columns = ['code', 'name', 'gender', 'company']

    filtered_data = user_data.drop(unwanted_columns, axis=1)

    scaler = preprocessing.MinMaxScaler()
    features_normal = scaler.fit_transform(filtered_data)

    """
    
    # Inertia analysis

    inertia = []
    sample_count = range(1, 10)
    for k in sample_count:
        k_mean_model = KMeans(n_clusters=k).fit(features_normal)
        k_mean_model.fit(features_normal)
        inertia.append(k_mean_model.inertia_)

    plt.plot(sample_count, inertia, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Inertia')
    plt.show()

    """

    k_means = KMeans(n_clusters=4).fit(features_normal)
    labels = k_means.labels_

    cluster = {0: [], 1: [], 2: [], 3: []}

    for idx, value in enumerate(labels):
        cluster[value].append(idx)

    with open("clustering_result.txt", "w") as file:
        for cluster_id in cluster.keys():
            file.write(str(cluster_id) + ":\n")
            file.write(str(cluster[cluster_id]) + "\n")
            

if __name__ == "__main__":

    try:
        main()
    except Exception as e:
        print("Exception occurred during execution : {}".format(e))
        raise e