from mongo_setup import db
from flask import Blueprint, request, jsonify, make_response, Response
from datetime import datetime
import io
import csv
import zipfile

datalog = Blueprint('datalog', __name__)

class DataLog:
    def __init__(self, createdAt, nome, cpf):
        self.createdAt = createdAt
        self.nome = nome
        self.cpf = cpf

    def save(self):
        collection = db['datalog']

        data = {
            'createdAt': self.createdAt,
            'nome': self.nome,
            'cpf': self.cpf
        }

        result = collection.insert_one(data)
        return result.inserted_id

    def __str__(self):
        return f"{self.createdAt} - {self.nome} - {self.cpf}"


@datalog.route('/datalog/upload', methods=['POST'])
def create():
    createdAt = request.form.get('createdAt')
    nome = request.form.get('nome')
    cpf = request.form.get('cpf')

    if not nome or not cpf:
        return make_response(jsonify({"error": "Nome e CPF são obrigatórios"}), 400)

    log = DataLog(createdAt, nome, cpf)
    log.save()

    return '', 200


@datalog.route('/datalog', methods=['GET'])
def get_all_data():
    query = {}
    collection = db['datalog']

    docs = list(collection.find(query))

    for log in docs:
        log['_id'] = str(log['_id'])
        log['createdAt'] = log['createdAt'].strftime("%Y-%m-%dT%H:%M:%SZ")

    return jsonify(docs)


@datalog.route('/datalog/latest-created', methods=['GET'])
def get_latest_created_data():
    collection = db['datalog']

    mais_recente = collection.find_one({}, sort=[('createdAt', -1)])

    if mais_recente is None:
        return make_response(jsonify({"error": "Nenhum dado encontrado"}), 404)

    data_mais_recente = mais_recente['createdAt']

    return jsonify({"latestCreatedAt": data_mais_recente.isoformat()})


@datalog.route('/datalog/cpf/count', methods=['GET'])
def count_by_cpf():
    collection = db['datalog']

    pipeline = [
        {"$group": {"_id": "$cpf", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {"cpf": "$_id", "_id": 0, "count": 1}}
    ]

    aggregation_result = list(collection.aggregate(pipeline))

    return jsonify(aggregation_result)


def get_all_documents():
    collection = db['datalog']
    documents = list(collection.find({}, {'_id': 0}))

    return documents


def generate_csv(documentos):
    output = io.StringIO()
    writer = csv.writer(output)

    if documentos:
        header = documentos[0].keys()
        writer.writerow(header)

    for doc in documentos:
        writer.writerow(doc.values())

    output.seek(0)
    return output


@datalog.route('/datalog/downloaddata', methods=['GET'])
def download_csv_zip():
    current_time = datetime.now()
    format_string = "%d%m%y_%H%M%S"
    formatted_time = current_time.strftime(format_string)
    filename_csv = "logs"

    documents = get_all_documents()

    csv_data = generate_csv(documents)

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr(f"{filename_csv}_{formatted_time}.csv", csv_data.getvalue())

    # Configurar a resposta HTTP
    response = Response(zip_buffer.getvalue())
    response.headers['Content-Type'] = 'application/zip'
    response.headers['Content-Disposition'] = f"attachment; filename={filename_csv}_{formatted_time}.zip"

    return response

@datalog.route('/datalog/downloadmergeddata', methods=['GET'])
def download_merged_data():
    filename_csv = "merged_data.csv"

    # Recupera os documentos e gera os dados CSV
    documents = get_all_documents()
    csv_data = generate_csv(documents)

    # Configura a resposta HTTP para enviar o arquivo .csv diretamente
    response = Response(csv_data.getvalue())
    response.headers['Content-Type'] = 'text/plain'
    response.headers['Content-Disposition'] = f"attachment; filename={filename_csv}"

    return response

