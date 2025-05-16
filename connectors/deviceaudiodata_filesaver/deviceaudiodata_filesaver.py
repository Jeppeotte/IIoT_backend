from fastapi import FastAPI, UploadFile, File, Form
from pathlib import Path
import sys
import logging
import uvicorn

app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)


#Directory for running locally
local_dir = "/home/jeppe/projectfolder"
mounted_dir = Path(local_dir)
#Directory for docker container
#mounted_dir = Path("/mounted_dir")

# Create the directory for the audiodata if it does not exist
audio_data_dir = mounted_dir.joinpath("data/audio_data")
audio_data_dir.mkdir(exist_ok=True,parents=True)

@app.post("/upload_audio")
async def upload_audio(file: UploadFile = File(...), device_id: str = Form(...)):
    # Creating the direction for the audio files for the device if it does not exist
    audio_file_dir = audio_data_dir.joinpath(device_id)
    audio_file_dir.mkdir(exist_ok=True, parents=True)
    # File name
    filename = file.filename
    file_path = audio_file_dir.joinpath(file.filename)

    # Creates the file
    with open(file_path, "wb") as out_file:
        content = await file.read()
        out_file.write(content)
    logger.info(f"Saved audio file from: {device_id} under directory {audio_file_dir} as {filename}")
    return {
        "message": "Upload successful",
        "saved_as": filename,
        "device_id": device_id
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)