#!/usr/bin/env python3

import csv
import random
import pandas as pd

def sample_and_annotate():
    # Read the CSV file
    df = pd.read_csv('data/output/instagram_data_classified.csv')

    # Sample 50 records where validated is True and 50 where validated is False
    true_samples = df[df['validated'] == True].sample(n=50, random_state=4675)
    false_samples = df[df['validated'] == False].sample(n=50, random_state=4675)

    # Combine samples
    samples = pd.concat([true_samples, false_samples], ignore_index=True)
    samples = samples.sample(frac=1)

    # Prepare results list
    results = []

    # Define questions
    questions = [
        "Does the text talk about climate change in general?",
        "Does the text talk about the causes of climate change?",
        "Does the text talk about the consequences of climate change?",
        "Does the text talk about solution to climate change (attenuation or adaptation)?",
        "Does the text talk about the collapse of biodiversity?",
        "Does the text talk about the causes of the collapse of biodiversity?",
        "Does the text talk about the consequences of the collapse of biodiversity?",
        "Does the text talk about the solutions for the collapse of biodiversity?",
        "Does the text talk about natural resources scarcity?",
        "Does the text talk about solutions to the natural resources scarcity problem?"
    ]

    print("Starting annotation process...")
    print(f"Total records to annotate: {len(samples)}")
    print("-" * 50)

    # Iterate through each sample
    for idx, row in samples.iterrows():
        print(f"\nRecord {idx + 1}/{len(samples)}")
        print(f"ID: {row['id']}")
        print(f"Text: {row['caption_text']}...")

        # Ask questions
        answers = []
        for question in questions:
            while True:
                answer = input(f"\n{question} (yes/no): ").strip().lower()
                if answer in ['yes', 'no']:
                    answers.append(answer)
                    break
                else:
                    print("Please answer 'yes' or 'no'")
        # Store results
        result = {
            'id': row['id'],
            'is_changement_climatique_gt': answers[0],
            'is_changement_climatique_causes_gt': answers[1],
            'is_changement_climatique_consequences_gt': answers[2],
            'is_climatique_solutions_gt': answers[3],
            'is_biodiversite_concepts_generaux_gt': answers[4],
            'is_biodiversite_causes_gt': answers[5],
            'is_biodiversite_consequences_gt': answers[6],
            'is_biodiversite_solutions_gt': answers[7],
            'is_ressources_gt': answers[8],
            'is_ressources_solutions_gt': answers[9],
            'is_adaptation_climatique_solutions': row['is_adaptation_climatique_solutions'],
            'is_attenuation_climatique_solutions': row['is_attenuation_climatique_solutions'],
            'is_biodiversite_causes': row['is_biodiversite_causes'],
            'is_biodiversite_concepts_generaux': row['is_biodiversite_concepts_generaux'],
            'is_biodiversite_consequences': row['is_biodiversite_consequences'],
            'is_biodiversite_solutions': row['is_biodiversite_solutions'],
            'is_changement_climatique_causes': row['is_changement_climatique_causes'],
            'is_changement_climatique_consequences': row['is_changement_climatique_consequences'],
            'is_changement_climatique_constat': row['is_changement_climatique_constat'],
            'is_ressources': row['is_ressources'],
            'is_ressources_solutions': row['is_ressources_solutions'],
        }
        results.append(result)

        print("-" * 50)

    # Save results to CSV
    output_file = 'data/output/instagram_annotated_samples.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            "id",
            "is_changement_climatique_gt",
            "is_changement_climatique_causes_gt",
            "is_changement_climatique_consequences_gt",
            "is_climatique_solutions_gt",
            "is_biodiversite_concepts_generaux_gt",
            "is_biodiversite_causes_gt",
            "is_biodiversite_consequences_gt",
            "is_biodiversite_solutions_gt",
            "is_ressources_gt",
            "is_ressources_solutions_gt",
            "is_adaptation_climatique_solutions",
            "is_attenuation_climatique_solutions",
            "is_biodiversite_causes",
            "is_biodiversite_concepts_generaux",
            "is_biodiversite_consequences",
            "is_biodiversite_solutions",
            "is_changement_climatique_causes",
            "is_changement_climatique_consequences",
            "is_changement_climatique_constat",
            "is_ressources",
            "is_ressources_solutions",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for result in results:
            writer.writerow(result)

    print(f"\nAnnotation completed! Results saved to {output_file}")

if __name__ == "__main__":
    sample_and_annotate()