from flask import Flask, request
import requests

app = Flask(__name__)

@app.route('/downloads/', methods=['GET', 'POST'])
def download():
    file_name = request.args['file_name'].replace("_", " ")  # user provides url in query string
    folder_path = request.args['folder_path']  # user provides url in query string
    uploads = os.path.join(current_app.root_path, app.config[folder_path])
    return send_from_directory(directory=uploads, filename= file_name)

parser.add_argument('-client', dest='client', action='store', required=True,
                    help='socket of the client')

client_ip = args.client.split(':')[0]
client_port = args.client.split(':')[1]

if __name__ == "__main__":
    app.run(port=str(client_port) + "0", debug=False)