import os, io
import logging
from moviepy import VideoFileClip
from typing import Optional
import tempfile

WHISPER_COLUMN_NAME = "plaintext_whisper"


def get_videofile_mp4_buffer(mp4_bytes: bytes):
    try:
        mp4_buffer = io.BytesIO(mp4_bytes)
        video = VideoFileClip(mp4_buffer)
        return video
    except Exception as e:
        logging.error(
            f"get_videofile_mp4_buffer - Error transforming MP4 bytes to VideoFileClip: {e}"
        )
        return None


# less cost to send audio instead of video to LLM
def transform_mp4_to_mp3(media: bytes) -> Optional[bytes]:
    logging.info("Transforming mp4 to mp3...")

    try:
        # Create a temporary file to write the MP4 bytes
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as temp_mp4_file:
            temp_mp4_file.write(media)  # Write the media (MP4 bytes) to the file
            temp_mp4_file.close()  # Close the file to ensure the data is written

            # Now use the temporary MP4 file with VideoFileClip
            video_clip = VideoFileClip(temp_mp4_file.name)

            # Convert the video clip's audio to MP3 and save it in memory
            with tempfile.NamedTemporaryFile(
                delete=False, suffix=".mp3"
            ) as temp_mp3_file:
                video_clip.audio.write_audiofile(temp_mp3_file.name)

                # Read the MP3 file content to return it as bytes
                with open(temp_mp3_file.name, "rb") as mp3_file:
                    mp3_data = mp3_file.read()

                # Clean up the temporary MP3 file
                os.remove(temp_mp3_file.name)

        # Clean up the temporary MP4 file
        os.remove(temp_mp4_file.name)

        return mp3_data

    except Exception as e:
        logging.error(f"Error transforming MP4 to MP3: {e}")
        return None
