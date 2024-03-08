import pandas as pd
import sqlalchemy
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient


def copy_blob(container_client: ContainerClient, source_blob_name: str, target_blob_name: str):
    """
    Copies the image named <source_blob_name> and saves it under <target_blob_name>.
    """
    source_blob_url = f"https://gcollection.blob.core.windows.net/card-originals/{source_blob_name}"
    copied_blob = container_client.get_blob_client(target_blob_name)
    print(f'Copying blob from {source_blob_name} to {target_blob_name}')
    copied_blob.start_copy_from_url(source_blob_url)


def main():
    """
    Takes the existing images in the card-originals bucket that are saved as <acronym>.jpg and saves the copies as <email>.jpg.
    """
    # Connect to Azure
    credential = DefaultAzureCredential()
    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient(account_url="https://gcollection.blob.core.windows.net", credential=credential)
    container_client = blob_service_client.get_container_client("card-originals")

    # Connect to local DB
    engine = sqlalchemy.create_engine('sqlite:///db.sqlite3')
    df = pd.read_sql('SELECT email, acronym FROM core_card', con=engine)
    for email, acronym in zip(df['email'], df['acronym']):
        source_blob_name = f'{acronym.lower()}.jpg'
        target_blob_name = f'{email}.jpg'
        print(f'Source Blob Name: {source_blob_name}, Target Blob Name: {target_blob_name}')

        # Uncomment the following line if you're serious
        # copy_blob(container_client, source_blob_name, target_blob_name)


if __name__ == "__main__":
    main();
