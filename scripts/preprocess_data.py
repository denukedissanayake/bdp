# Google colab code to preprocess the data

import pandas as pd
from google.colab import files

uploaded = files.upload()

filename = list(uploaded.keys())[0]

df = pd.read_csv(filename)
date_col = df.columns[1] if 'date' in df.columns else df.columns[0]

def to_dmy(x):
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%m/%d/%Y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except:
            pass
    return pd.NaT

df['date_DMY'] = df[date_col].apply(to_dmy).dt.strftime("%d/%m/%Y")

output_filename = "weatherData_updated.csv"
df.to_csv(output_filename, index=False)
files.download(output_filename)