import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient


class AzureBlobStorage:
    def __init__(self):
       
        load_dotenv()
     
        account_name =  os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
        account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        container_name =  os.environ.get("AZURE_STORAGE_CONTAINER_NAME")

        if not (account_name and account_key and container_name):
            raise ValueError("Certifique-se de definir as variáveis de ambiente corretas.")

        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.blob_service_client = BlobServiceClient(
            account_url=f"https://{account_name}.blob.core.windows.net",
            credential=account_key
        )
        

    def upload_file(self, data, blob_name):
        try:
            container_client = self.blob_service_client.get_container_client(self.container_name)

            # Certifique-se de que os dados sejam bytes antes do upload
            if isinstance(data, str):
                data = data.encode('utf-8')

            container_client.upload_blob(name=blob_name, data=data)
            print("Upload concluído com sucesso.")
        except Exception as e:
            print(f"Erro durante o upload: {e}")
            return False
            


    def download_blob(self, blob_name, local_file_path=None):
        try:
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=blob_name)
            
            if local_file_path:
                with open(local_file_path, "wb") as data:
                    data.write(blob_client.download_blob().readall())
                print(f"Download concluído com sucesso. Salvo em {local_file_path}")
            else:
                content = blob_client.download_blob().readall()
                return content
        except Exception as e:
            print(f"Erro durante o download: {e}")
