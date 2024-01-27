import os
import json
import requests
from multiprocessing import Process

COVERS_PATH = f"{os.path.abspath(os.path.dirname(__file__))}/covers"
LAST_RUN_JSON = F"{os.path.abspath(os.path.dirname(__file__))}/last_run.json"

def is_downloadable(url: str):
	h = requests.head(url)
	header = h.headers
	content_type = header.get('content-type')
	if 'text' in content_type.lower():
		return False
	if 'html' in content_type.lower():
		return False
	return True

def cover_download(image_url: str, serial: str):
	try:
		if is_downloadable(image_url):
			file_path = f"{COVERS_PATH}/{serial}.jpg"
			with requests.get(image_url, stream=True) as r:
				with open(file_path, 'wb') as f:
					for chunk in r.iter_content(chunk_size=1024):
						if chunk:
							f.write(chunk)
			return file_path
		else:
			print(f'Url {image_url} is not downloadable.')
	except Exception as ex:
		print(f"Error downloading {image_url} for serial {serial}.")
		print(f"Exception: {ex}")

def main():
	processes = []
	with open(LAST_RUN_JSON) as json_file:
		json_data = json.load(json_file)
		for item in json_data:
			cover_serial = item.get('serial')
			cover_image_url = item.get('image_urls')
			cover_image_url = cover_image_url[0] if cover_image_url else None
			if os.path.isfile(f"{COVERS_PATH}/{cover_serial}.jpg"):
				continue
			print(f"Add process to download serial: {cover_serial} url: {cover_image_url}")
			process = Process(target=cover_download,args=(cover_image_url,cover_serial,))
			processes.append(process)
		
	# Start the processes
	for process in processes:
		process.start()

	# Ensure all processes are done and list their time to download as well
	for process in processes:
		process.join()

if __name__ == "__main__":
	main()