import os
import json
import csv
from tqdm import tqdm

def extract_data_from_json_files():
    # Define the base directory containing JSON files
    json_dir = './data/json/'
    output_file = './data/input/instagram_data.csv'

    # Open CSV file for writing
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        # Define CSV column headers
        fieldnames = ['file_name', 'user_pk', 'user_username', 'user_full_name', 'user_id', 'like_count', 'comment_count', 'caption_text']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header row
        writer.writeheader()

        # Iterate through all subdirectories in the base directory
        for subdir in os.listdir(json_dir):
            subdir_path = os.path.join(json_dir, subdir)

            # Check if it's a directory and starts with 'medias_'
            if os.path.isdir(subdir_path) and subdir.startswith('medias_'):
                # Iterate through all JSON files in the subdirectory
                for filename in tqdm(os.listdir(subdir_path)):
                    if filename.endswith('.json') and filename != 'state.json':
                        file_path = os.path.join(subdir_path, filename)

                        try:
                            # Read the JSON file
                            with open(file_path, 'r', encoding='utf-8') as jsonfile:
                                data = json.load(jsonfile)

                            # Extract the required fields
                            user_pk = data.get('user', {}).get('pk', '')
                            user_username = data.get('user', {}).get('username', '')
                            user_full_name = data.get('user', {}).get('full_name', '')
                            user_id = data.get('user', {}).get('id', '')
                            like_count = data.get('like_count', 0)
                            comment_count = data.get('comment_count', 0)
                            caption_text = data.get('caption_text', '')

                            # Write to CSV
                            writer.writerow({
                                'file_name': filename,
                                'user_pk': user_pk,
                                'user_username': user_username,
                                'user_full_name': user_full_name,
                                'user_id': user_id,
                                'like_count': like_count,
                                'comment_count': comment_count,
                                'caption_text': caption_text.replace("\n", " ")
                            })

                        except Exception as e:
                            print(f"Error processing {filename}: {str(e)}")
                            # Continue with the next file even if there's an error
                            continue

    print(f"Data extraction complete. Results saved to {output_file}")

if __name__ == "__main__":
    extract_data_from_json_files()