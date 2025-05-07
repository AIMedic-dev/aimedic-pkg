import requests
import pandas as pd
from tqdm.auto import tqdm
import csv

print("Bienvenido al script de anonimización de AIMEDIC")

def split_csv_in_chunks(file_path, chunk_size):
    for chunk in pd.read_csv(
        file_path,
        chunksize=chunk_size,
        encoding='latin1',
        delimiter=';'
    ):
        yield chunk

def upload_csv(file_path, api_key, chunk_size):
    url = "https://anonymization-ia-gwhab3dsdwggffhp.canadacentral-01.azurewebsites.net/anonimizar_csv/"
    headers = {
        "API-Key": api_key
    }

    responses = []
    for chunk in split_csv_in_chunks(file_path, chunk_size):
        # Save chunk to temporary file
        temp_file = "temp_chunk.csv"
        chunk.to_csv(temp_file, index=False, sep=';', encoding='latin1')
        
        files = {
            'file': open(temp_file, 'rb')
        }

        response = requests.post(url, headers=headers, files=files)
        files['file'].close()

        if response.status_code == 200:
            print("Chunk uploaded successfully.")
            try:
                responses.append(response)
            except requests.exceptions.JSONDecodeError:
                print("Error decoding JSON response.")
                return None
        else:
            print(f"Failed to upload chunk. Status code: {response.status_code}")
            return None

    return "".join([str(response.content) for response in responses])    


def test_csv(file_path, api_key):
    url = "https://anonymization-ia-gwhab3dsdwggffhp.canadacentral-01.azurewebsites.net/anonimizar_csv_con_reporte/"
    #url = "http://localhost:8000/anonimizar_csv_con_reporte/"
    headers = {"API-Key": api_key}
    files = {"file": open(file_path, "rb")}

    resp = requests.post(url, headers=headers, files=files)
    resp.raise_for_status()
    with open("anonimizado.zip", "wb") as f:
        f.write(resp.content)


if __name__ == "__main__":
    #clean_csv_file("Muestra AIMEDIC(Sheet1)3.csv", "Muestra AIMEDIC(Sheet1)3_clean.csv")
    responses = upload_csv("Muestra AIMEDIC(Sheet1)3.csv", "cd-sdk-5237", 2)
    print(responses)