import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or

# cred =  credentials.Certificate(r"C:\Users\Micro\Documents\Firebase\sdkfirebase.json")
cred_oficial = credentials.Certificate(r"C:\Users\Micro\Documents\Firebase\SDKAdminFirebaseOficial.json")

import schedule
from schedule import repeat, every, run_pending
import wget
import os
import shutil
import time 

# Inicializa o firebase (entra no firebase)
# firebase_admin.initialize_app(cred)
firebase_admin.initialize_app(cred_oficial)
db = firestore.client()
# index_rvc = 1

name_collection = 'BancoDadosRVC'

def update_index_rvc():
    collection_ref = db.collection('RVCs_Sinc')
    doc_ref = collection_ref.document('xLA6yOcWuCRfE42K8iEN')

    doc = {}
    for i in collection_ref.stream():
        doc = i.to_dict()
    index_rvc = doc['index_rvc']

    doc_ref.update({'index_rvc': index_rvc + 1})
    return index_rvc
def get_documents_with_pdf(collectionName):
    try:
        doc_ref = db.collection(collectionName)
        dict_out = {"list_num_proposta": [], "list_url": [], "list_data_visita": [], "list_nome": []}

        # Solicitação com filtro de pdf vazio
        query = doc_ref.where(filter= FieldFilter('url_PDF', '!=', ''))

        docs = query.stream()

        for doc in docs:
            data = doc.to_dict()
            # Verifica se não está sincronizado, adiciona a url na lista e depois atualiza a situação da variavel para true
            if data['sinc_server'] == False:
                dict_out['list_num_proposta'].append(data['num_proposta'])
                dict_out['list_url'].append(data['url_PDF'])
                dict_out['list_data_visita'].append(data['data_hora_agendada'])
                dict_out['list_nome'].append(data['solicitante'])
                update_field_sinc(doc.id)
        return dict_out
    except Exception as ex:
        print(f'Erro ao selecionar documentos: {str(ex)}')
def update_field_sinc(documentID):
    try:
        collection_ref = db.collection(name_collection)
        doc_ref = collection_ref.document(documentID)

        # Atualiza o campo sinc para true
        doc_ref.update({'sinc_server': True})
    except Exception as ex:
        print(f'Erro ao atualizar campo: {str(ex)}')
def download_pdf(dictData):
    try:
        path_arch   = "C:/Users/Micro/Desktop/ENVIAR_PDF_SERVIDOR"
        path_server = "Z:/03-DRIVE CLIENTES/MOSAIC/02-CMA/RVC"

        list_url  = dictData['list_url']
        list_num_proposta = dictData['list_num_proposta']

        # Tratamento de strings na lista de hora da visita
        list_data_agendada_gross =  dictData['list_data_visita']
        list_data_agendada = [s[0:10].replace("/", "-") for s in list_data_agendada_gross]
        
        # Baixa cada url da lista de urls
        for i, url in enumerate(list_url):
            
            name_pdf = f"{list_num_proposta[i]}_RVC-{update_index_rvc()}_{list_data_agendada[i]}"
            wget.download(url, out=f"{name_pdf}.pdf")

            shutil.move(f"{path_arch}/{name_pdf}.pdf", f"{path_server}/{name_pdf}.pdf")
       
    except Exception as ex:
        print(f"Não foi possível realizar o download :(")
        print(str(ex))

@repeat(every(10).seconds)
def schedule_download():
    dict_url = get_documents_with_pdf(name_collection)
    download_pdf(dict_url)

# schedule.every(30).seconds.do(schedule_download)

while True:
   run_pending()
   time.sleep(1)