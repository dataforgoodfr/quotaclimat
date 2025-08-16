# %%
import json

import pandas as pd

with open("label_mapping.json", "r", encoding="utf-8") as f:
    label_mapping = json.load(f)

def load_annotations(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    rows = []
    for task in data:
        task_id = task.get('id')
        text = task.get('data', {}).get('plaintext', "")
        channel_title = task.get('data', {}).get('channel_title', "")
        data_start = task.get('data', {}).get('start', "")

        annotations = task.get('annotations', [])
        for ann in annotations:
            for result in ann.get('result', []):
                if result.get('type') == 'labels':
                    snippet = result['value']['text']
                    label = result['value']['labels'][0]
                    start = result['value']['start']
                    end = result['value']['end']

                    rows.append({
                        'task_id': task_id,
                        'channel_title': channel_title,
                        'data_start': data_start,
                        'plaintext': text,
                        'text': snippet,
                        'label': label,
                        'start': start,
                        'end': end
                    })

    return pd.DataFrame(rows)

def main():
    df = load_annotations('data/labelstudio_export.json')
    df['label'] = df['label'].map(label_mapping)

    dataset = pd.read_csv('data/france3_merged_plain_text.csv', index_col=0).reset_index(drop=True)

    # On ne garde que les pubs qui ont été annotées dans le dataset France3
    labelled_dataset = dataset[dataset.start.isin(df.data_start.unique())]
    labelled_dataset.to_csv('data/ads_to_be_labelled_france3.csv')

    # On sauvegarde les annotations
    df.to_csv('data/labelstudio_ads_france3.csv')

if __name__ == "__main__":
    main()
# %%
