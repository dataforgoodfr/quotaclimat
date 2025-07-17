# Ads Analysis

This content is a preliminary exploration to detect then classify ads from the Media Observatory on Ecology

It relies on two differents streams:

- **Chunk Classification**: Detecting the chunks than contains advertising - see advertising_segmentation.py
- **Ads Segmentation**: Spliting the differents ads, then classifying between several topics - see advertising_classification.py

### File structure

```bash
├───best_model - the best classification model used yet
│   ├───1_Pooling
│   └───2_Normalize
├───checkpoints - transformers checkpoints
│   └───checkpoint-625
│       └───1_Pooling
├───data
│   ├───intermediate
│   ├───processed
│   └───raw
├───secrets - for API keys
├─── create_datasets.py - create a dataset prior to training classification 
├─── advertising_classification_SetFit.py - exploration to detect chunks with advertising
├─── advertising_classification_Camembert.py - exploration to detect chunks with advertising, not working yet
├─── advertising_segmentation.py - exploration to split and classify ads
├─── model_exploration.py - mini script to join different outputs and visualize their differences on Metabase
```

### Quickstart 

- Install dependencies via requirements.txt
- Choose wether you want to work on chunk classification or ads segmentation
- If you're using segmentation, create an api_key.txt file with your API key in it