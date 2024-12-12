from flask import Flask, request

from datalog import datalog

app = Flask(__name__)
app.config.from_pyfile('config.py')

app.register_blueprint(datalog)


@app.route('/alive', methods=['GET'])
def alive():
    return "alive"



def get_client_ip():
    if 'X-Forwarded-For' in request.headers:
        user_ip = request.headers['X-Forwarded-For']
    else:
        user_ip = request.remote_addr
    return user_ip

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8002)