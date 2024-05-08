from flask import Flask,request

from config import vm_port
from database.db import hide_flat


def create_app():
    app = Flask(__name__)

    @app.route('/')
    def index():
        return "Hello, World!"

    @app.route('/update')
    def update_db():
        item_id = request.args.get('id')
        result = ''
        if item_id:
            result = hide_flat(item_id)
        return f"{result}"

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=vm_port)